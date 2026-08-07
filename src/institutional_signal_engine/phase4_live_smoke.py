"""Alpaca-catalogued, ThetaData-streamed Phase 4 pilot smoke runner."""

import argparse
import asyncio
import json
import os
import subprocess
from asyncio import Semaphore
from collections import Counter
from datetime import datetime, timedelta
from time import monotonic
from uuid import uuid4
from zoneinfo import ZoneInfo

from .config import Settings
from .contract_mapping import AlpacaOptionContract, map_alpaca_contract, round_trip_validate
from .liquidity import PHASE4_OBSERVATION_SPREAD_POLICY_VERSION, finalize_liquidity
from .live_smoke import _secret
from .live_smoke import run as run_signal_smoke
from .persistence import InMemoryRepository, PostgresRepository
from .providers.alpaca import AlpacaEquitiesProvider
from .providers.alpaca_option_snapshots import AlpacaOptionSnapshotProvider
from .providers.alpaca_options import AlpacaOptionsContractProvider
from .providers.common import ProviderError
from .providers.thetadata import ThetaContract
from .providers.thetadata_open_interest import (
    OpenInterestEvidence,
    ThetaDataOpenInterestProvider,
)
from .startup import StageCallback, StageRecorder, StartupTimeout, bounded_startup
from .universe import (
    PHASE4_SWEEP_OBSERVATION_POLICY_VERSION,
    PILOT_SYMBOLS,
    AlpacaContractSelector,
    UniverseManifest,
    allocate_subscription_capacity,
)


def _load_settings() -> Settings:
    runtime_file = os.environ.get(
        "RUNTIME_ENV_FILE", "/etc/institutional-signal-engine/runtime.env"
    )
    settings = (
        Settings.from_env_file(runtime_file)
        if os.path.exists(runtime_file)
        else Settings.from_env()
    )
    if settings.trading_enabled:
        raise RuntimeError("phase4_live_smoke refuses trading-enabled configuration")
    return settings


async def run(
    seconds: float,
    skip_oi_diagnostic: bool = False,
    startup_timeout_seconds: float | None = None,
    stage_callback: StageCallback | None = None,
) -> dict[str, object]:
    recorder = StageRecorder(stage_callback)
    settings = _load_settings()
    recorder.emit("configuration_loaded")
    startup_timeout = startup_timeout_seconds or float(settings.phase4_startup_timeout_seconds)

    def startup_remaining() -> float:
        return max(0.001, startup_timeout - recorder.elapsed_seconds)

    alpaca = AlpacaEquitiesProvider(
        settings.alpaca_data_url,
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    catalog = AlpacaOptionsContractProvider(
        "https://paper-api.alpaca.markets",
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    run_id = uuid4()
    as_of = datetime.now(ZoneInfo("America/New_York")).date()
    try:
        recorder.emit(
            "universe_discovery_started", completed_items=0, remaining_items=len(PILOT_SYMBOLS)
        )
        prices = await bounded_startup(
            alpaca.current_prices(PILOT_SYMBOLS),
            recorder,
            "equity_price_snapshots",
            startup_remaining(),
        )
        discovered_list: list[AlpacaOptionContract] = []

        async def discover() -> None:
            async for contract in catalog.discover_active_calls(
                PILOT_SYMBOLS,
                limit=100,
                expiration_date_gte=as_of + timedelta(days=7),
                expiration_date_lte=as_of + timedelta(days=45),
            ):
                discovered_list.append(contract)

        await bounded_startup(
            discover(), recorder, "alpaca_contract_pagination", startup_remaining()
        )
        discovered = tuple(discovered_list)
        recorder.emit(
            "universe_discovery_completed",
            completed_items=len(discovered),
            remaining_items=0,
        )
        recorder.emit("providers_authenticated")
    except ProviderError as exc:
        recorder.emit("report_emitted", status="blocked_provider", error_category=exc.category)
        return {
            "run_id": str(run_id),
            "trading_enabled": False,
            "status": "blocked_provider",
            "provider": "alpaca",
            "reason": exc.category,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }
    mapping_results = tuple(
        round_trip_validate(map_alpaca_contract(contract)) for contract in discovered
    )
    coarse_selections, coarse_exclusions = AlpacaContractSelector().coarse_select(
        mapping_results,
        prices,
        as_of,
        symbols=PILOT_SYMBOLS,
        max_per_symbol=settings.phase4_pre_enrichment_max_per_symbol,
    )
    coarse_contracts = {
        contract for selection in coarse_selections for contract in selection.contracts
    }
    if len(coarse_contracts) > settings.phase4_max_enrichment_candidates:
        raise RuntimeError("phase4_enrichment_candidate_limit_exceeded")
    mapping_by_contract = {
        result.theta_contract: result
        for result in mapping_results
        if result.accepted and result.theta_contract in coarse_contracts
    }
    quote_provider = AlpacaOptionSnapshotProvider(
        "https://data.alpaca.markets",
        _secret(settings.alpaca_key_id),
        _secret(settings.alpaca_secret_key),
    )
    recorder.emit("enrichment_started", completed_items=0, remaining_items=len(mapping_by_contract))
    try:
        quote_evidence = await bounded_startup(
            quote_provider.snapshots(
                tuple(
                    (result.source.occ_symbol, result.canonical)
                    for result in mapping_by_contract.values()
                    if result.canonical is not None
                ),
                feed="opra",
            ),
            recorder,
            "alpaca_quote_enrichment",
            startup_remaining(),
        )
    except ProviderError as exc:
        recorder.emit(
            "report_emitted", status="blocked_quote_provider", error_category=exc.category
        )
        return {
            "run_id": str(run_id),
            "trading_enabled": False,
            "status": "blocked_quote_provider",
            "reason": exc.category,
            "coarse_shortlisted_contracts": len(coarse_contracts),
            "orders_constructed": 0,
            "orders_submitted": 0,
        }
    assert isinstance(quote_evidence, tuple)
    quote_by_contract = {
        evidence.identity.theta_contract(): evidence for evidence in quote_evidence
    }
    recorder.emit("enrichment_completed", completed_items=len(quote_evidence), remaining_items=0)
    oi_provider = ThetaDataOpenInterestProvider(settings.theta_terminal_http_url)
    aapl_contract = next(
        (
            contract
            for selection in coarse_selections
            if selection.symbol == "AAPL"
            for contract in selection.contracts
        ),
        None,
    )
    oi_evidence: dict[ThetaContract, OpenInterestEvidence] = {}
    oi_diagnostic: dict[str, object] = {
        "request_timestamp": datetime.now(ZoneInfo("America/New_York")).isoformat(),
        "endpoint": "/v3/option/snapshot/open_interest",
        "status": "not_attempted",
    }
    try:
        oi_diagnostic["mdss"] = await bounded_startup(
            oi_provider.mdss_status(), recorder, "thetadata_mdss_status", startup_remaining()
        )
    except ProviderError as exc:
        oi_diagnostic["mdss"] = {"status": "unavailable", "reason": exc.category}
    if skip_oi_diagnostic:
        oi_diagnostic.update(
            {
                "status": "skipped_after_prior_http_500",
                "endpoint_unavailable": True,
                "reason": "endpoint_unavailable",
            }
        )
    elif aapl_contract is not None:
        target_contract = aapl_contract
        try:
            first_oi = await oi_provider.snapshot(target_contract)
        except ProviderError as exc:
            oi_diagnostic.update(
                {"status": "failed", "reason": exc.category, **oi_provider.last_diagnostic}
            )
            oi_diagnostic["endpoint_unavailable"] = True
            oi_diagnostic["status"] = "endpoint_unavailable_observation_continues"
            oi_diagnostic["reason"] = "endpoint_unavailable"
        else:
            oi_evidence[target_contract] = first_oi
            oi_diagnostic.update(
                {
                    "status": "success",
                    "canonical_contract": {
                        "root": target_contract.root,
                        "expiration": target_contract.expiration,
                        "strike": target_contract.strike,
                        "right": target_contract.right,
                    },
                    "oi_value": first_oi.open_interest,
                    "reported_at": first_oi.reported_at.isoformat(),
                    "effective_date": first_oi.effective_date.isoformat(),
                }
            )
    semaphore = Semaphore(5)
    rate_lock = asyncio.Lock()
    next_oi_request = 0.0

    async def enrich_oi(
        contract: ThetaContract,
    ) -> tuple[ThetaContract, OpenInterestEvidence | None]:
        nonlocal next_oi_request
        async with semaphore:
            async with rate_lock:
                delay = next_oi_request - monotonic()
                if delay > 0:
                    await asyncio.sleep(delay)
                next_oi_request = monotonic() + float(settings.phase4_oi_request_interval_seconds)
            try:
                return contract, await oi_provider.snapshot(contract)
            except ProviderError:
                return contract, None

    remaining = [contract for contract in coarse_contracts if contract not in oi_evidence]
    if remaining and oi_diagnostic.get("status") == "success":
        for contract, evidence in await asyncio.gather(
            *(enrich_oi(contract) for contract in remaining)
        ):
            if evidence is not None:
                oi_evidence[contract] = evidence
    enrichment = finalize_liquidity(
        coarse_selections,
        mapping_results,
        quote_by_contract,
        oi_evidence,
        datetime.now(ZoneInfo("America/New_York")),
        max_quote_age_seconds=settings.phase4_quote_freshness_seconds,
        maximum_spread=settings.thresholds.maximum_spread,
        minimum_quote_size=settings.phase4_min_quote_size,
        alpaca_open_interest={
            result.theta_contract: result.source.open_interest
            for result in mapping_results
            if result.accepted and result.theta_contract is not None
        },
        require_open_interest=False,
        policy_version=PHASE4_SWEEP_OBSERVATION_POLICY_VERSION,
        spread_policy_version=PHASE4_OBSERVATION_SPREAD_POLICY_VERSION,
    )
    selections = enrichment.selections
    allocation = allocate_subscription_capacity(
        selections,
        prices,
        trade_limit=settings.phase4_trade_subscription_limit,
        quote_limit=settings.phase4_quote_subscription_limit,
        max_contracts_per_symbol=settings.phase4_max_contracts_per_symbol,
    )
    plan = allocation.selected
    recorder.emit("selection_completed", completed_items=len(plan), remaining_items=0)
    recorder.emit("subscription_planning_completed", completed_items=len(plan), remaining_items=0)
    selected_contracts = set(plan)
    contract_metadata = {
        next(
            contract
            for contract in selection.contracts
            if contract.root == str(item["root"])
            and contract.expiration == int(str(item["expiration"]))
            and contract.strike == int(str(item["strike"]))
            and contract.right == str(item["right"])
        ): item
        for selection in selections
        for item in selection.contract_evidence
        if any(
            contract.root == str(item["root"])
            and contract.expiration == int(str(item["expiration"]))
            and contract.strike == int(str(item["strike"]))
            and contract.right == str(item["right"])
            for contract in selection.contracts
        )
    }
    diagnostic_membership = {
        "discovered_universe": {
            result.theta_contract
            for result in mapping_results
            if result.accepted and result.theta_contract is not None
        },
        "coarse_shortlist": coarse_contracts,
        "enrichment_set": coarse_contracts,
        "final_subscription_plan": selected_contracts,
        "submitted_request_registry": selected_contracts,
        "acknowledged_registry": set(),
    }
    try:
        engine_commit = (
            await asyncio.to_thread(
                subprocess.run,
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                check=True,
            )
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        engine_commit = None
    manifest = UniverseManifest(
        run_id,
        as_of,
        tuple(PILOT_SYMBOLS),
        selections,
        plan,
        mapping_records=tuple(result.record() for result in mapping_results),
        capacity_excluded=allocation.capacity_excluded,
        coarse_exclusions=coarse_exclusions,
        enrichment_records=enrichment.records,
        engine_commit=engine_commit,
        sector_by_symbol={symbol: "XLK" for symbol in PILOT_SYMBOLS},
        synchronization_symbols=tuple(
            selection.symbol for selection in selections if selection.included
        ),
    )
    repository = (
        PostgresRepository(settings.database_url) if settings.database_url else InMemoryRepository()
    )
    if isinstance(repository, PostgresRepository):
        await bounded_startup(
            asyncio.to_thread(repository.initialize),
            recorder,
            "database_connected",
            startup_remaining(),
        )
    recorder.emit("database_connected")
    repository.record_universe(manifest.record())
    if isinstance(repository, PostgresRepository):
        repository.flush()
    enrichment_rejection_counts: Counter[str] = Counter()
    for record in enrichment.records:
        reasons = record.get("rejection_reasons")
        if isinstance(reasons, tuple):
            enrichment_rejection_counts.update(str(reason) for reason in reasons)
    report: dict[str, object] = {
        "run_id": str(run_id),
        "trading_enabled": False,
        "symbols_evaluated": list(PILOT_SYMBOLS),
        "symbols_included": [selection.symbol for selection in selections if selection.included],
        "symbols_excluded": {
            selection.symbol: list(selection.rejection_reasons)
            for selection in selections
            if not selection.included
        },
        "alpaca_contracts_received": len(discovered),
        "contracts_accepted": sum(1 for result in mapping_results if result.accepted),
        "contracts_rejected": sum(1 for result in mapping_results if not result.accepted),
        "mapping_failures": [
            {"field": field, "reason": reason, "count": count}
            for (field, reason), count in sorted(
                Counter(
                    (result.failed_field or "unknown", result.rejection_reason or "unknown")
                    for result in mapping_results
                    if not result.accepted
                ).items()
            )
        ],
        "selection_counts_by_symbol": {
            selection.symbol: {
                "catalog_contracts": sum(
                    result.source.underlying_symbol == selection.symbol
                    for result in mapping_results
                ),
                "coarse_shortlisted_contracts": sum(
                    len(item.contracts)
                    for item in coarse_selections
                    if item.symbol == selection.symbol
                ),
                "quote_enriched_contracts": sum(
                    record["symbol"] == selection.symbol
                    and isinstance(record["rejection_reasons"], tuple)
                    and not any(
                        "quote_snapshot" in str(reason) for reason in record["rejection_reasons"]
                    )
                    for record in enrichment.records
                ),
                "oi_enriched_contracts": sum(
                    evidence.get("oi_available_at_subscription") is True
                    for evidence in selection.contract_evidence
                ),
                "oi_blocked_contracts": sum(
                    evidence.get("oi_available_at_subscription") is not True
                    for evidence in selection.contract_evidence
                ),
                "final_selected_contracts": len(selection.contracts),
                "quote_liquidity_passed_contracts": sum(
                    record["symbol"] == selection.symbol and not record.get("rejection_reasons")
                    for record in enrichment.records
                    if "symbol" in record
                ),
            }
            for selection in selections
        },
        "coarse_excluded_counts": dict(Counter(str(item["reason"]) for item in coarse_exclusions)),
        "enrichment_rejection_counts": dict(enrichment_rejection_counts),
        "enrichment_spread_diagnostics": list(enrichment.diagnostics),
        "oi_diagnostic": oi_diagnostic,
        "selected_contracts": [
            {
                "symbol": selection.symbol,
                "expiration": selection.expiration,
                "contracts": [
                    {
                        "root": contract.root,
                        "expiration": contract.expiration,
                        "strike": contract.strike,
                        "right": contract.right,
                    }
                    for contract in selection.contracts
                    if contract in selected_contracts
                ],
            }
            for selection in selections
            if selection.included
        ],
        "subscription_counts": {
            "requested": len(allocation.requested),
            "selected": len(allocation.selected),
            "capacity_excluded": len(allocation.capacity_excluded),
            "trade_submitted": len(allocation.trade_plan),
            "quote_submitted": len(allocation.quote_plan),
            "trade_acknowledged": 0,
            "quote_acknowledged": 0,
            "trade_rejected": 0,
            "quote_rejected": 0,
            "rejected_or_unmatched": 0,
            "trade_limit": settings.phase4_trade_subscription_limit,
            "quote_limit": settings.phase4_quote_subscription_limit,
        },
        "capacity_excluded": list(allocation.capacity_excluded),
        "policy_version": PHASE4_SWEEP_OBSERVATION_POLICY_VERSION,
        "subscription_plan_count": len(plan),
        "manifest_persisted": True,
        "orders_constructed": 0,
        "orders_submitted": 0,
    }
    if not plan:
        report["status"] = "blocked_no_contracts_selected"
        report["reason"] = "quote_observation_evidence_unavailable"
        recorder.emit("report_emitted", status=report["status"])
        return report
    signal_report = await run_signal_smoke(
        seconds,
        symbols=tuple(selection.symbol for selection in selections if selection.included),
        contracts=plan,
        request_types=("TRADE", "QUOTE"),
        contract_metadata=contract_metadata,
        diagnostic_membership=diagnostic_membership,
        startup_timeout_seconds=startup_timeout,
        stage_callback=lambda stage, fields: recorder.emit(stage, **fields),
    )
    report.update(signal_report)
    raw_sync_symbols = signal_report.get("synchronized_symbols", ())
    sync_symbols = (
        tuple(str(symbol) for symbol in raw_sync_symbols)
        if isinstance(raw_sync_symbols, (list, tuple))
        else ()
    )
    raw_incomplete = signal_report.get("incomplete_state_reasons", {})
    incomplete_reasons = (
        {
            str(symbol): tuple(str(reason) for reason in reasons)
            for symbol, reasons in raw_incomplete.items()
            if isinstance(reasons, (list, tuple))
        }
        if isinstance(raw_incomplete, dict)
        else {}
    )
    raw_acknowledgement = signal_report.get("subscription_acknowledgement", {})
    raw_rejected_diagnostics = (
        raw_acknowledgement.get("rejected_event_diagnostics", [])
        if isinstance(raw_acknowledgement, dict)
        else []
    )
    raw_acknowledgements = (
        raw_acknowledgement.get("request_registry", [])
        if isinstance(raw_acknowledgement, dict)
        else []
    )
    final_manifest = UniverseManifest(
        run_id,
        as_of,
        tuple(PILOT_SYMBOLS),
        selections,
        plan,
        mapping_records=tuple(result.record() for result in mapping_results),
        acknowledgements=tuple(item for item in raw_acknowledgements if isinstance(item, dict)),
        capacity_excluded=allocation.capacity_excluded,
        coarse_exclusions=coarse_exclusions,
        enrichment_records=(*enrichment.records, *enrichment.diagnostics),
        engine_commit=engine_commit,
        sector_by_symbol={symbol: "XLK" for symbol in PILOT_SYMBOLS},
        synchronization_symbols=sync_symbols,
        incomplete_state_reasons=incomplete_reasons,
        rejected_event_diagnostics=tuple(
            item for item in raw_rejected_diagnostics if isinstance(item, dict)
        ),
    )
    repository.record_universe(final_manifest.record())
    if isinstance(repository, PostgresRepository):
        repository.flush()
    acknowledgement = signal_report.get("subscription_acknowledgement", {})
    if isinstance(acknowledgement, dict):
        requests_by_type = acknowledgement.get("requests_by_type", {})
        if isinstance(requests_by_type, dict) and isinstance(report["subscription_counts"], dict):
            report["subscription_counts"].update(
                {
                    "trade_acknowledged": int(
                        requests_by_type.get("TRADE", {}).get("acknowledged", 0)
                    ),
                    "quote_acknowledged": int(
                        requests_by_type.get("QUOTE", {}).get("acknowledged", 0)
                    ),
                    "trade_rejected": int(
                        requests_by_type.get("TRADE", {}).get("rejected_or_unmatched", 0)
                    ),
                    "quote_rejected": int(
                        requests_by_type.get("QUOTE", {}).get("rejected_or_unmatched", 0)
                    ),
                    "rejected_or_unmatched": len(acknowledgement.get("rejected_or_unmatched", [])),
                }
            )
    report["status"] = "live_observation_complete"
    recorder.emit("observation_completed", status=report["status"])
    recorder.emit("persistence_drained")
    recorder.emit("report_emitted", status=report["status"])
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--seconds", type=float, default=60.0)
    parser.add_argument("--skip-oi-diagnostic", action="store_true")
    parser.add_argument("--startup-timeout-seconds", type=float, default=None)
    arguments = parser.parse_args()
    try:
        result = asyncio.run(
            asyncio.wait_for(
                run(
                    arguments.seconds,
                    arguments.skip_oi_diagnostic,
                    arguments.startup_timeout_seconds,
                ),
                timeout=(arguments.seconds + 2 * (arguments.startup_timeout_seconds or 120) + 30),
            )
        )
    except StartupTimeout as exc:
        result = exc.report()
        print(json.dumps(result, sort_keys=True), flush=True)
        raise SystemExit(2)
    except TimeoutError:
        result = {
            "status": "total_command_timeout",
            "error_category": "total_command_timeout",
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }
        print(json.dumps(result, sort_keys=True), flush=True)
        raise SystemExit(2)
    except (ProviderError, RuntimeError, ValueError) as exc:
        result = {
            "status": "blocked",
            "reason": type(exc).__name__,
            "trading_enabled": False,
            "orders_constructed": 0,
            "orders_submitted": 0,
        }
    print(json.dumps(result, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
