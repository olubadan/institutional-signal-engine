"""Typed option-universe selection and reconciliation."""

import hashlib
import json
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import Protocol, cast
from uuid import UUID

from .contract_mapping import MappingResult
from .providers.thetadata import ThetaContract

PILOT_SYMBOLS: tuple[str, ...] = (
    "MMM",
    "AOS",
    "ABT",
    "ABBV",
    "ACN",
    "ADBE",
    "AMD",
    "AES",
    "AFL",
    "A",
    "APD",
    "ABNB",
    "AKAM",
    "ALB",
    "ARE",
    "ALGN",
    "ALLE",
    "LNT",
    "ALL",
    "GOOGL",
    "GOOG",
    "MO",
    "AMZN",
    "AMCR",
    "AEE",
    "AEP",
    "AXP",
    "AIG",
    "AMT",
    "AWK",
    "AMP",
    "AME",
    "AMGN",
    "APH",
    "ADI",
    "AON",
    "APA",
    "APO",
    "AAPL",
    "AMAT",
    "APP",
    "APTV",
    "ACGL",
    "ADM",
    "ARES",
    "ANET",
    "AJG",
    "AIZ",
    "T",
    "ATO",
    "ADSK",
    "ADP",
    "AZO",
    "AVB",
    "AVY",
    "AXON",
    "BKR",
    "BALL",
    "BAC",
    "BAX",
    "BDX",
    "BBY",
    "TECH",
    "BIIB",
    "BLK",
    "BX",
    "XYZ",
    "BNY",
    "BA",
    "BKNG",
    "BSX",
    "BMY",
    "AVGO",
    "BR",
    "BRO",
    "BLDR",
    "BG",
    "BXP",
    "CHRW",
    "CDNS",
    "CPT",
    "COF",
    "CAH",
    "CCL",
    "CARR",
    "CVNA",
    "CASY",
    "CAT",
    "CBOE",
    "CBRE",
    "CDW",
    "COR",
    "CNC",
    "CNP",
    "CF",
    "CRL",
    "SCHW",
    "CHTR",
    "CVX",
    "CMG",
    "CB",
    "CHD",
    "CIEN",
    "CI",
    "CINF",
    "CTAS",
    "CSCO",
    "C",
    "CFG",
    "CLX",
    "CME",
    "CMS",
    "KO",
    "CTSH",
    "COHR",
    "COIN",
    "CL",
    "CMCSA",
    "FIX",
    "COP",
    "ED",
    "STZ",
    "CEG",
    "COO",
    "CPRT",
    "GLW",
    "CPAY",
    "CTVA",
    "CSGP",
    "COST",
    "CRH",
    "CRWD",
    "CCI",
    "CSX",
    "CMI",
    "CVS",
    "DHR",
    "DRI",
    "DDOG",
    "DVA",
    "DECK",
    "DE",
    "DELL",
    "DAL",
    "DVN",
    "DXCM",
    "FANG",
    "DLR",
    "DG",
    "DLTR",
    "D",
    "DPZ",
    "DASH",
    "DOV",
    "DOW",
    "DHI",
    "DTE",
    "DUK",
    "DD",
    "ETN",
    "EBAY",
    "ECHO",
    "ECL",
    "EIX",
    "EW",
    "ELV",
    "EME",
    "EMR",
    "ETR",
    "EOG",
    "EQT",
    "EFX",
    "EQIX",
    "EQR",
    "ERIE",
    "ESS",
    "EL",
    "EG",
    "EVRG",
    "ES",
    "EXC",
    "EXE",
    "EXPE",
    "EXPD",
    "EXR",
    "XOM",
    "FFIV",
    "FDS",
    "FICO",
    "FAST",
    "FRT",
    "FDX",
    "FDXF",
    "FERG",
    "FIS",
    "FITB",
    "FSLR",
    "FE",
    "FISV",
    "FLEX",
    "F",
    "FTNT",
    "FTV",
    "FOXA",
    "FOX",
    "BEN",
    "FCX",
    "GRMN",
    "IT",
    "GE",
    "GEHC",
    "GEV",
    "GEN",
    "GNRC",
    "GD",
    "GIS",
    "GM",
    "GPC",
    "GILD",
    "GPN",
    "GL",
    "GDDY",
    "GS",
    "HAL",
    "HIG",
    "HAS",
    "HCA",
    "DOC",
    "HSIC",
    "HSY",
    "HPE",
    "HLT",
    "HD",
    "HONA",
    "HON",
    "HRL",
    "HST",
    "HWM",
    "HPQ",
    "HUBB",
    "HUM",
    "HBAN",
    "HII",
    "IBM",
    "IEX",
    "IDXX",
    "ITW",
    "INCY",
    "IR",
    "PODD",
    "INTC",
    "IBKR",
    "ICE",
    "IFF",
    "IP",
    "INTU",
    "ISRG",
    "IVZ",
    "INVH",
    "IQV",
    "IRM",
    "JBHT",
    "JBL",
    "JKHY",
    "J",
    "JNJ",
    "JCI",
    "JPM",
    "KVUE",
    "KDP",
    "KEY",
    "KEYS",
    "KMB",
    "KIM",
    "KMI",
    "KKR",
    "KLAC",
    "KHC",
    "KR",
    "LHX",
    "LH",
    "LRCX",
    "LVS",
    "LDOS",
    "LEN",
    "LII",
    "LLY",
    "LIN",
    "LYV",
    "LMT",
    "L",
    "LOW",
    "LULU",
    "LITE",
    "LYB",
    "MTB",
    "MPC",
    "MAR",
    "MRSH",
    "MLM",
    "MRVL",
    "MAS",
    "MA",
    "MKC",
    "MCD",
    "MCK",
    "MDT",
    "MRK",
    "META",
    "MET",
    "MTD",
    "MGM",
    "MCHP",
    "MU",
    "MSFT",
    "MAA",
    "MRNA",
    "TAP",
    "MDLZ",
    "MPWR",
    "MNST",
    "MCO",
    "MS",
    "MOS",
    "MSI",
    "MSCI",
    "NDAQ",
    "NTAP",
    "NFLX",
    "NEM",
    "NWSA",
    "NWS",
    "NEE",
    "NKE",
    "NI",
    "NDSN",
    "NSC",
    "NTRS",
    "NOC",
    "NCLH",
    "NRG",
    "NUE",
    "NVDA",
    "NVR",
    "NXPI",
    "ORLY",
    "OXY",
    "ODFL",
    "OMC",
    "ON",
    "OKE",
    "ORCL",
    "OTIS",
    "PCAR",
    "PKG",
    "PLTR",
    "PANW",
    "PSKY",
    "PH",
    "PAYX",
    "PYPL",
    "PNR",
    "PEP",
    "PFE",
    "PCG",
    "PM",
    "PSX",
    "PNW",
    "PNC",
    "PPG",
    "PPL",
    "PFG",
    "PG",
    "PGR",
    "PLD",
    "PRU",
    "PEG",
    "PTC",
    "PSA",
    "PHM",
    "PWR",
    "QCOM",
    "DGX",
    "Q",
    "RL",
    "RJF",
    "RTX",
    "O",
    "REG",
    "REGN",
    "RF",
    "RSG",
    "RMD",
    "RVTY",
    "HOOD",
    "ROK",
    "ROL",
    "ROP",
    "ROST",
    "RCL",
    "SPGI",
    "CRM",
    "SNDK",
    "SBAC",
    "SLB",
    "STX",
    "SRE",
    "NOW",
    "SHW",
    "SPG",
    "SWKS",
    "SJM",
    "SW",
    "SNA",
    "SOLV",
    "SO",
    "LUV",
    "SWK",
    "SBUX",
    "STT",
    "STLD",
    "STE",
    "SYK",
    "SMCI",
    "SYF",
    "SNPS",
    "SYY",
    "TMUS",
    "TROW",
    "TTWO",
    "TPR",
    "TRGP",
    "TGT",
    "TEL",
    "TDY",
    "TER",
    "TSLA",
    "TXN",
    "TPL",
    "TXT",
    "TMO",
    "TJX",
    "TKO",
    "TTD",
    "TSCO",
    "TT",
    "TDG",
    "TRV",
    "TRMB",
    "TFC",
    "TYL",
    "TSN",
    "USB",
    "UBER",
    "UDR",
    "ULTA",
    "UNP",
    "UAL",
    "UPS",
    "URI",
    "UNH",
    "UHS",
    "VLO",
    "VEEV",
    "VTR",
    "VLTO",
    "VRSN",
    "VRSK",
    "VZ",
    "VRTX",
    "VRT",
    "VTRS",
    "VICI",
    "V",
    "VST",
    "VMC",
    "WRB",
    "GWW",
    "WAB",
    "WMT",
    "DIS",
    "WBD",
    "WM",
    "WAT",
    "WEC",
    "WFC",
    "WELL",
    "WST",
    "WDC",
    "WY",
    "WSM",
    "WMB",
    "WTW",
    "WDAY",
    "WYNN",
    "XEL",
    "XYL",
    "YUM",
    "ZBRA",
    "ZBH",
    "ZTS",
)
PHASE4_OBSERVATION_POLICY_VERSION = "phase4-observation-liquidity-relaxation-v1"
PHASE4_SWEEP_OBSERVATION_POLICY_VERSION = "phase4-sweep-observation-without-oi-v1"
PHASE4_LIQUIDITY_EVIDENCE_SOURCE = "OWNER_APPROVED_PHASE4_PILOT"


@dataclass(frozen=True)
class ContractEvidence:
    contract: ThetaContract
    underlying_price: Decimal
    bid: Decimal | None
    ask: Decimal | None
    trade_volume: int | None
    open_interest: int | None


class OptionUniverseProvider(Protocol):
    def expirations(self, symbol: str, as_of: int) -> tuple[int, ...]: ...
    def strikes(self, symbol: str, expiration: int) -> tuple[int, ...]: ...
    def evidence(
        self, symbol: str, expiration: int, strikes: tuple[int, ...]
    ) -> tuple[ContractEvidence, ...]: ...


class ContractUniverseSelector:
    def __init__(self, max_contracts: int = 15_000) -> None:
        self.max_contracts = max_contracts

    def select_calls(
        self, evidence: tuple[ContractEvidence, ...], today: int
    ) -> tuple[ThetaContract, ...]:
        valid = [
            item
            for item in evidence
            if item.contract.right == "C"
            and item.contract.expiration >= today
            and item.trade_volume is not None
            and item.trade_volume > 0
            and item.open_interest is not None
            and item.open_interest > 0
            and item.bid is not None
            and item.ask is not None
            and item.ask >= item.bid
        ]
        valid.sort(
            key=lambda item: (
                item.contract.expiration,
                abs(Decimal(item.contract.strike) / Decimal(1000) - item.underlying_price),
                -int(item.trade_volume or 0),
                item.contract.strike,
            )
        )
        return tuple(item.contract for item in valid[: self.max_contracts])


@dataclass(frozen=True)
class SymbolEligibilityEvidence:
    symbol: str
    market_cap: Decimal | None
    average_daily_dollar_volume: Decimal | None
    listed_options: bool | None
    average_options_volume: Decimal | None

    def evaluate(self) -> tuple[bool, tuple[str, ...]]:
        reasons: list[str] = []
        if self.market_cap is None or self.market_cap < Decimal(10000000000):
            reasons.append("market_cap_below_10b_or_missing")
        if self.average_daily_dollar_volume is None or self.average_daily_dollar_volume < Decimal(
            50000000
        ):
            reasons.append("average_daily_dollar_volume_below_50m_or_missing")
        if self.listed_options is not True:
            reasons.append("listed_options_missing_or_false")
        if self.average_options_volume is None or self.average_options_volume < Decimal(2000):
            reasons.append("average_options_volume_below_2000_or_missing")
        return not reasons, tuple(reasons)


@dataclass(frozen=True)
class UniverseSelection:
    symbol: str
    included: bool
    expiration: int | None
    contracts: tuple[ThetaContract, ...]
    rejection_reasons: tuple[str, ...]
    provenance: tuple[str, ...]
    provider_responses: tuple[dict[str, object], ...]
    contract_evidence: tuple[dict[str, object], ...] = ()
    symbol_liquidity_evidence_source: str | None = None
    symbol_liquidity_verified: bool = True
    policy_version: str | None = None


@dataclass(frozen=True)
class UniverseManifest:
    run_id: UUID
    session_date: date
    input_symbols: tuple[str, ...]
    selections: tuple[UniverseSelection, ...]
    ordered_subscription_plan: tuple[ThetaContract, ...]
    mapping_records: tuple[dict[str, object], ...] = ()
    acknowledgements: tuple[dict[str, object], ...] = ()
    reconnect_history: tuple[dict[str, object], ...] = ()
    capacity_excluded: tuple[dict[str, object], ...] = ()
    policy_version: str = PHASE4_OBSERVATION_POLICY_VERSION
    coarse_exclusions: tuple[dict[str, object], ...] = ()
    enrichment_records: tuple[dict[str, object], ...] = ()
    selection_version: str = "phase4-universe-v1"
    mapping_version: str = "alpaca-occ-thetadata-v1"
    engine_commit: str | None = None
    sector_by_symbol: dict[str, str] | None = None
    synchronization_symbols: tuple[str, ...] = ()
    incomplete_state_reasons: dict[str, tuple[str, ...]] | None = None
    rejected_event_diagnostics: tuple[dict[str, object], ...] = ()
    coverage_plan: dict[str, object] | None = None
    coverage_candidate_population_version: str | None = None
    contract_transitions: tuple[dict[str, object], ...] = ()
    finalization: dict[str, object] | None = None

    def record(self) -> dict[str, object]:
        return {
            "run_id": str(self.run_id),
            "session_date": self.session_date.isoformat(),
            "input_symbols": list(self.input_symbols),
            "selections": list(
                selection_audits(self.run_id, self.selections, self.ordered_subscription_plan)
            ),
            "subscription_plan": [
                {
                    "root": contract.root,
                    "expiration": contract.expiration,
                    "strike": contract.strike,
                    "right": contract.right,
                }
                for contract in self.ordered_subscription_plan
            ],
            "mapping_records": list(self.mapping_records),
            "acknowledgements": list(self.acknowledgements),
            "reconnect_history": list(self.reconnect_history),
            "capacity_excluded": list(self.capacity_excluded),
            "selection_version": self.selection_version,
            "mapping_version": self.mapping_version,
            "policy_version": self.policy_version,
            "coarse_exclusions": list(self.coarse_exclusions),
            "enrichment_records": list(self.enrichment_records),
            "engine_commit": self.engine_commit,
            "sector_by_symbol": self.sector_by_symbol or {},
            "synchronization_symbols": list(self.synchronization_symbols),
            "incomplete_state_reasons": {
                symbol: list(reasons)
                for symbol, reasons in (self.incomplete_state_reasons or {}).items()
            },
            "rejected_event_diagnostics": list(self.rejected_event_diagnostics),
            "coverage_plan": self.coverage_plan or {},
            "coverage_candidate_population_version": self.coverage_candidate_population_version,
            "contract_transitions": list(self.contract_transitions),
            "finalization": self.finalization or {},
        }


class UniverseLimitExceeded(ValueError):
    """The plan cannot be connected because it exceeds the Standard limit."""


class Phase4UniverseSelector:
    """Deterministically select the bounded liquid pilot universe."""

    def __init__(
        self,
        option_provider: OptionUniverseProvider,
        max_contracts: int = 15_000,
        max_moneyness_distance: Decimal = Decimal("0.20"),
    ) -> None:
        self.option_provider = option_provider
        self.max_contracts = max_contracts
        self.max_moneyness_distance = max_moneyness_distance

    def select_symbol(self, evidence: SymbolEligibilityEvidence, today: int) -> UniverseSelection:
        symbol = evidence.symbol.upper()
        eligible, reasons = evidence.evaluate()
        if not eligible:
            return UniverseSelection(symbol, False, None, (), reasons, ("eligibility",), ())
        responses: list[dict[str, object]] = []
        for expiration in sorted(self.option_provider.expirations(symbol, today)):
            if expiration < today:
                continue
            strikes = tuple(sorted(set(self.option_provider.strikes(symbol, expiration))))
            responses.append(
                {
                    "endpoint": "strikes",
                    "symbol": symbol,
                    "expiration": expiration,
                    "count": len(strikes),
                }
            )
            candidate_evidence = self.option_provider.evidence(symbol, expiration, strikes)
            selected = tuple(
                sorted(
                    {
                        item.contract
                        for item in candidate_evidence
                        if self._qualifies_contract(item)
                    },
                    key=lambda contract: (contract.expiration, contract.strike, contract.right),
                )
            )
            if selected:
                return UniverseSelection(
                    symbol,
                    True,
                    expiration,
                    selected,
                    (),
                    ("eligibility", "nearest_future_expiration", "liquidity", "moneyness"),
                    tuple(responses),
                )
        return UniverseSelection(
            symbol,
            False,
            None,
            (),
            ("no_liquid_near_term_call_contract",),
            ("eligibility", "expiration_discovery", "strike_discovery", "liquidity", "moneyness"),
            tuple(responses),
        )

    def select_pilot(
        self,
        evidence: tuple[SymbolEligibilityEvidence, ...],
        today: int,
    ) -> tuple[UniverseSelection, ...]:
        selections = tuple(
            self.select_symbol(item, today)
            for item in sorted(evidence, key=lambda value: value.symbol)
        )
        total = sum(len(item.contracts) for item in selections)
        if total > self.max_contracts:
            raise UniverseLimitExceeded(f"subscription_plan_exceeds_{self.max_contracts}_contracts")
        return selections

    def _qualifies_contract(self, item: ContractEvidence) -> bool:
        if item.contract.right != "C" or item.trade_volume is None or item.trade_volume < 1:
            return False
        if item.open_interest is None or item.open_interest < 1:
            return False
        if item.bid is None or item.ask is None or item.bid <= 0 or item.ask < item.bid:
            return False
        if item.underlying_price <= 0:
            return False
        strike_dollars = Decimal(item.contract.strike) / Decimal(1000)
        return (
            abs(strike_dollars - item.underlying_price) / item.underlying_price
            <= self.max_moneyness_distance
        )


class AlpacaContractSelector:
    """Select canonical contracts using only Alpaca discovery records."""

    def __init__(
        self,
        min_days_to_expiration: int = 7,
        max_days_to_expiration: int = 45,
        max_moneyness_distance: Decimal = Decimal("0.20"),
        policy_version: str = PHASE4_OBSERVATION_POLICY_VERSION,
    ) -> None:
        if policy_version != PHASE4_OBSERVATION_POLICY_VERSION:
            raise ValueError("phase4_observation_policy_required")
        self.min_days = min_days_to_expiration
        self.max_days = max_days_to_expiration
        self.max_moneyness_distance = max_moneyness_distance
        self.policy_version = policy_version

    def select(
        self,
        results: tuple[MappingResult, ...],
        underlying_prices: dict[str, Decimal],
        as_of: date,
        symbols: Iterable[str] = (),
    ) -> tuple[UniverseSelection, ...]:
        by_symbol: dict[str, list[MappingResult]] = {}
        failures: dict[str, list[str]] = {}
        expected_symbols = {symbol.upper() for symbol in symbols}
        for result in results:
            symbol = result.source.underlying_symbol
            if not result.accepted or result.canonical is None:
                failures.setdefault(symbol, []).append(
                    result.rejection_reason or "mapping_rejected"
                )
                continue
            canonical = result.canonical
            dte = (result.source.expiration - as_of).days
            raw = result.source.original_fields
            if canonical.right != "C":
                failures.setdefault(symbol, []).append("calls_only")
            elif not self.min_days <= dte <= self.max_days:
                failures.setdefault(symbol, []).append("expiration_outside_configured_window")
            elif symbol not in underlying_prices or underlying_prices[symbol] <= 0:
                failures.setdefault(symbol, []).append("underlying_price_unavailable")
            elif result.source.open_interest is None or result.source.open_interest <= 0:
                failures.setdefault(symbol, []).append("missing_or_zero_open_interest")
            elif (
                result.source.open_interest_date is not None
                and result.source.open_interest_date > as_of
            ):
                failures.setdefault(symbol, []).append("open_interest_date_in_future")
            elif raw.get("average_options_volume") is not None and Decimal(
                str(raw["average_options_volume"])
            ) < Decimal(2000):
                failures.setdefault(symbol, []).append("average_options_volume_below_2000")
            elif raw.get("bid") is None or raw.get("ask") is None:
                failures.setdefault(symbol, []).append("quote_liquidity_unavailable")
            else:
                strike = Decimal(canonical.strike) / Decimal(1000)
                distance = abs(strike - underlying_prices[symbol]) / underlying_prices[symbol]
                if distance > self.max_moneyness_distance:
                    failures.setdefault(symbol, []).append("moneyness_outside_configured_range")
                else:
                    by_symbol.setdefault(symbol, []).append(result)
        selections: list[UniverseSelection] = []
        all_symbols = expected_symbols | set(by_symbol) | set(failures)
        for symbol in sorted(all_symbols):
            if symbol not in by_symbol and symbol not in failures:
                failures[symbol] = ["no_alpaca_contracts_returned"]
            valid_results = [
                item
                for item in by_symbol.get(symbol, [])
                if item.canonical is not None and item.theta_contract is not None
            ]

            def sort_key(
                item: MappingResult, selected_symbol: str = symbol
            ) -> tuple[date, Decimal, int]:
                assert item.canonical is not None
                return (
                    item.source.expiration,
                    abs(
                        Decimal(item.canonical.strike) / Decimal(1000)
                        - underlying_prices[selected_symbol]
                    ),
                    item.canonical.strike,
                )

            candidates = sorted(
                valid_results,
                key=sort_key,
            )
            expiration = candidates[0].source.expiration if candidates else None
            selected_items = tuple(
                item
                for item in candidates
                if item.source.expiration == expiration and item.theta_contract is not None
            )
            selected = tuple(item.theta_contract for item in selected_items if item.theta_contract)
            contract_evidence = tuple(
                cast(
                    dict[str, object],
                    {
                        "root": item.theta_contract.root,
                        "expiration": item.theta_contract.expiration,
                        "strike": item.theta_contract.strike,
                        "right": item.theta_contract.right,
                        "open_interest": item.source.open_interest,
                        "oi_date_source": (
                            "ALPACA_DATED"
                            if item.source.open_interest_date is not None
                            else "ALPACA_UNDATED"
                        ),
                        "open_interest_verified_as_of": item.source.open_interest_date is not None,
                        "evidence_quality": (
                            "PRODUCTION_QUALIFIED"
                            if item.source.open_interest_date is not None
                            else "PHASE4_OBSERVATIONAL"
                        ),
                        "symbol_liquidity_evidence_source": PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                        "symbol_liquidity_verified": False,
                        "policy_version": self.policy_version,
                    },
                )
                for item in selected_items
                if item.theta_contract is not None
            )
            reasons = tuple(sorted(set(failures.get(symbol, ()))))
            selections.append(
                UniverseSelection(
                    symbol,
                    bool(selected),
                    int(expiration.strftime("%Y%m%d")) if expiration else None,
                    tuple(dict.fromkeys(selected)),
                    () if selected else reasons or ("no_contracts_selected",),
                    (
                        "alpaca_contract_discovery",
                        "canonical_mapping",
                        "open_interest_observation_policy",
                        "liquidity_evidence",
                        "moneyness",
                    ),
                    (),
                    contract_evidence,
                    PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                    False,
                    self.policy_version,
                )
            )
        return tuple(selections)

    def coarse_select(
        self,
        results: tuple[MappingResult, ...],
        underlying_prices: dict[str, Decimal],
        as_of: date,
        symbols: Iterable[str] = (),
        max_per_symbol: int = 25,
    ) -> tuple[tuple[UniverseSelection, ...], tuple[dict[str, object], ...]]:
        """Build a bounded shortlist before quote/OI enrichment."""
        if max_per_symbol < 1:
            raise ValueError("max_per_symbol must be positive")
        candidates: dict[str, list[MappingResult]] = {}
        exclusions: list[dict[str, object]] = []
        seen_canonical: set[ThetaContract] = set()

        def exclusion_record(
            result: MappingResult, reason: str, **extra: object
        ) -> dict[str, object]:
            canonical = result.canonical
            return {
                "symbol": result.source.underlying_symbol,
                "provider_symbol": result.source.occ_symbol,
                "canonical_identity": (
                    {
                        "root": canonical.root,
                        "expiration": canonical.expiration,
                        "strike": canonical.strike,
                        "right": canonical.right,
                    }
                    if canonical is not None
                    else None
                ),
                "canonical_identity_available": canonical is not None,
                "selection_stage": "COARSE_SHORTLIST",
                "reason": reason,
                **extra,
            }

        for result in results:
            symbol = result.source.underlying_symbol
            reason: str | None = None
            if not result.accepted or result.canonical is None or result.theta_contract is None:
                reason = (
                    "CANONICAL_IDENTITY_UNAVAILABLE"
                    if result.canonical is None
                    else result.rejection_reason or "mapping_rejected"
                )
            elif result.canonical.right != "C":
                reason = "calls_only"
            elif result.theta_contract in seen_canonical:
                reason = "DUPLICATE_CANONICAL_IDENTITY"
            elif not self.min_days <= (result.source.expiration - as_of).days <= self.max_days:
                reason = "expiration_outside_configured_window"
            elif symbol not in underlying_prices or underlying_prices[symbol] <= 0:
                reason = "underlying_price_unavailable"
            else:
                strike = Decimal(result.canonical.strike) / Decimal(1000)
                if (
                    abs(strike - underlying_prices[symbol]) / underlying_prices[symbol]
                    > self.max_moneyness_distance
                ):
                    reason = "moneyness_outside_configured_range"
            if reason is not None:
                exclusions.append(exclusion_record(result, reason))
            else:
                assert result.theta_contract is not None
                seen_canonical.add(result.theta_contract)
                candidates.setdefault(symbol, []).append(result)
        expected = {symbol.upper() for symbol in symbols}
        selections: list[UniverseSelection] = []
        for symbol in sorted(expected | set(candidates)):

            def coarse_key(
                item: MappingResult, selected_symbol: str = symbol
            ) -> tuple[date, Decimal, int]:
                assert item.canonical is not None
                return (
                    item.source.expiration,
                    abs(
                        Decimal(item.canonical.strike) / Decimal(1000)
                        - underlying_prices[selected_symbol]
                    ),
                    item.canonical.strike,
                )

            ordered = sorted(
                candidates.get(symbol, []),
                key=coarse_key,
            )
            nearest = ordered[0].source.expiration if ordered else None
            eligible = [item for item in ordered if item.source.expiration == nearest]
            chosen = eligible[:max_per_symbol]
            for rank, item in enumerate(eligible[max_per_symbol:], max_per_symbol + 1):
                exclusions.append(
                    exclusion_record(
                        item,
                        "COARSE_SHORTLIST_CAPACITY_EXCLUDED",
                        rank=rank,
                    )
                )
            for item in ordered:
                if item not in eligible:
                    exclusions.append(
                        exclusion_record(item, "COARSE_SHORTLIST_EXPIRATION_EXCLUDED")
                    )
            contracts = tuple(item.theta_contract for item in chosen if item.theta_contract)
            evidence = tuple(
                {
                    "root": contract.root,
                    "expiration": contract.expiration,
                    "strike": contract.strike,
                    "right": contract.right,
                    "selection_stage": "COARSE_SHORTLIST",
                    "provider_symbol": item.source.occ_symbol,
                }
                for item, contract in zip(chosen, contracts, strict=True)
            )
            selections.append(
                UniverseSelection(
                    symbol,
                    bool(contracts),
                    int(nearest.strftime("%Y%m%d")) if nearest else None,
                    contracts,
                    () if contracts else ("no_coarse_shortlist_contract",),
                    (
                        "alpaca_contract_discovery",
                        "canonical_mapping",
                        "nearest_eligible_expiration",
                        "moneyness",
                        "selection_stage:COARSE_SHORTLIST",
                    ),
                    (),
                    evidence,
                    PHASE4_LIQUIDITY_EVIDENCE_SOURCE,
                    False,
                    PHASE4_OBSERVATION_POLICY_VERSION,
                )
            )
        return tuple(selections), tuple(exclusions)


def subscription_plan(selections: tuple[UniverseSelection, ...]) -> tuple[ThetaContract, ...]:
    """Return a stable, duplicate-free Standard subscription plan."""
    contracts = {
        contract
        for selection in selections
        if selection.included
        for contract in selection.contracts
    }
    return tuple(
        sorted(
            contracts, key=lambda value: (value.root, value.expiration, value.strike, value.right)
        )
    )


@dataclass(frozen=True)
class CapacityAllocation:
    requested: tuple[ThetaContract, ...]
    selected: tuple[ThetaContract, ...]
    trade_plan: tuple[ThetaContract, ...]
    quote_plan: tuple[ThetaContract, ...]
    capacity_excluded: tuple[dict[str, object], ...]


def allocate_subscription_capacity(
    selections: tuple[UniverseSelection, ...],
    underlying_prices: dict[str, Decimal],
    trade_limit: int = 15_000,
    quote_limit: int = 10_000,
    max_contracts_per_symbol: int = 1_000,
) -> CapacityAllocation:
    """Format the planner's final selection; never make a second decision."""
    del underlying_prices
    selected_tuple = tuple(
        contract
        for selection in selections
        if selection.included
        for contract in selection.contracts
    )
    return format_subscription_plan(
        selected_tuple,
        trade_limit=trade_limit,
        quote_limit=quote_limit,
        max_contracts_per_symbol=max_contracts_per_symbol,
    )


def format_subscription_plan(
    selected: tuple[ThetaContract, ...],
    trade_limit: int = 15_000,
    quote_limit: int = 10_000,
    max_contracts_per_symbol: int = 1_000,
) -> CapacityAllocation:
    """Format and validate an already-selected planner allocation."""
    if min(trade_limit, quote_limit, max_contracts_per_symbol) < 1:
        raise ValueError("subscription limits must be positive")
    selected_tuple = tuple(selected)
    if len(set(selected_tuple)) != len(selected_tuple):
        raise ValueError("duplicate_subscription_contract")
    if len(selected_tuple) > min(trade_limit, quote_limit):
        raise ValueError("subscription_capacity_exceeded")
    per_symbol = Counter(contract.root for contract in selected_tuple)
    if any(count > max_contracts_per_symbol for count in per_symbol.values()):
        raise ValueError("subscription_symbol_capacity_exceeded")
    return CapacityAllocation(
        requested=selected_tuple,
        selected=selected_tuple,
        trade_plan=selected_tuple,
        quote_plan=selected_tuple,
        capacity_excluded=(),
    )


def selection_audits(
    run_id: UUID,
    selections: tuple[UniverseSelection, ...],
    plan: tuple[ThetaContract, ...],
) -> tuple[dict[str, object], ...]:
    """Create sanitized, replayable audit payloads for every pilot symbol."""
    plan_values = [
        {
            "root": contract.root,
            "expiration": contract.expiration,
            "strike": contract.strike,
            "right": contract.right,
        }
        for contract in plan
    ]
    return tuple(
        {
            "run_id": str(run_id),
            "symbol": selection.symbol,
            "included": selection.included,
            "expiration": selection.expiration,
            "contracts": [
                {
                    "root": contract.root,
                    "expiration": contract.expiration,
                    "strike": contract.strike,
                    "right": contract.right,
                }
                for contract in selection.contracts
            ],
            "rejection_reasons": list(selection.rejection_reasons),
            "provenance": list(selection.provenance),
            "provider_responses": list(selection.provider_responses),
            "contract_evidence": list(selection.contract_evidence),
            "symbol_liquidity_evidence_source": selection.symbol_liquidity_evidence_source,
            "symbol_liquidity_verified": selection.symbol_liquidity_verified,
            "policy_version": selection.policy_version,
            "capacity_excluded": [],
            "subscription_plan": plan_values,
        }
        for selection in selections
    )


def reconcile(
    previous: tuple[ThetaContract, ...], selected: tuple[ThetaContract, ...]
) -> tuple[tuple[ThetaContract, ...], tuple[ThetaContract, ...]]:
    old, new = set(previous), set(selected)
    return tuple(
        sorted(new - old, key=lambda value: (value.expiration, value.strike, value.right))
    ), tuple(sorted(old - new, key=lambda value: (value.expiration, value.strike, value.right)))


PLANNER_EPOCH_LIFECYCLE_STATES = ("pending", "active", "superseded", "finalized")
PLANNER_EPOCH_VERSION = "planner-epoch-v1"


@dataclass(frozen=True)
class PlannerEpoch:
    """Immutable, content-addressed planner epoch.

    Every universe transition produces a new PlannerEpoch. Epochs are
    content-hashed for deterministic comparison and idempotent reapplication.
    Only one epoch may be active at a time.
    """

    epoch_id: str
    sequence: int
    effective_at: datetime
    candidate_population_version: str
    selected_contracts: tuple[ThetaContract, ...]
    trade_subscriptions: tuple[ThetaContract, ...]
    quote_subscriptions: tuple[ThetaContract, ...]
    additions: tuple[ThetaContract, ...]
    removals: tuple[ThetaContract, ...]
    planner_version: str
    content_hash: str
    provenance: str
    lifecycle: str
    coverage_plan: dict[str, object] | None = None
    selection_records: tuple[dict[str, object], ...] = ()

    @classmethod
    def create(
        cls,
        sequence: int,
        effective_at: datetime,
        candidate_population_version: str,
        selected_contracts: tuple[ThetaContract, ...],
        previous_contracts: tuple[ThetaContract, ...] = (),
        provenance: str = "production-planner-v1",
        coverage_plan: dict[str, object] | None = None,
        selection_records: tuple[dict[str, object], ...] = (),
    ) -> "PlannerEpoch":
        """Create a new PlannerEpoch with deterministic content hash.

        The content hash covers every field that affects replay identity.
        Additions and removals are computed from the diff against the previous
        epoch's selected contracts.
        """
        additions, removals = reconcile(previous_contracts, selected_contracts)
        trade_subscriptions = tuple(
            sorted(selected_contracts, key=lambda v: (v.root, v.expiration, v.strike, v.right))
        )
        quote_subscriptions = trade_subscriptions  # paired TRADE/QUOTE
        pre_hash = {
            "sequence": sequence,
            "effective_at": effective_at.isoformat(),
            "candidate_population_version": candidate_population_version,
            "selected_contracts": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in selected_contracts
            ],
            "trade_subscriptions": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in trade_subscriptions
            ],
            "quote_subscriptions": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in quote_subscriptions
            ],
            "additions": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in additions
            ],
            "removals": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in removals
            ],
            "planner_version": PLANNER_EPOCH_VERSION,
            "provenance": provenance,
        }
        canonical = json.dumps(pre_hash, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        content_hash = hashlib.sha256(canonical.encode()).hexdigest()
        return cls(
            epoch_id=str(UUID(content_hash[:32])),
            sequence=sequence,
            effective_at=effective_at,
            candidate_population_version=candidate_population_version,
            selected_contracts=selected_contracts,
            trade_subscriptions=trade_subscriptions,
            quote_subscriptions=quote_subscriptions,
            additions=additions,
            removals=removals,
            planner_version=PLANNER_EPOCH_VERSION,
            content_hash=content_hash,
            provenance=provenance,
            lifecycle="pending",
            coverage_plan=coverage_plan,
            selection_records=selection_records,
        )

    def activate(self) -> "PlannerEpoch":
        """Return a copy with lifecycle='active'. Immutable — returns new object."""
        if self.lifecycle != "pending":
            raise ValueError(f"Cannot activate epoch in state {self.lifecycle}")
        return PlannerEpoch(
            epoch_id=self.epoch_id,
            sequence=self.sequence,
            effective_at=self.effective_at,
            candidate_population_version=self.candidate_population_version,
            selected_contracts=self.selected_contracts,
            trade_subscriptions=self.trade_subscriptions,
            quote_subscriptions=self.quote_subscriptions,
            additions=self.additions,
            removals=self.removals,
            planner_version=self.planner_version,
            content_hash=self.content_hash,
            provenance=self.provenance,
            lifecycle="active",
            coverage_plan=self.coverage_plan,
            selection_records=self.selection_records,
        )

    def supersede(self) -> "PlannerEpoch":
        """Return a copy with lifecycle='superseded'."""
        return PlannerEpoch(
            epoch_id=self.epoch_id,
            sequence=self.sequence,
            effective_at=self.effective_at,
            candidate_population_version=self.candidate_population_version,
            selected_contracts=self.selected_contracts,
            trade_subscriptions=self.trade_subscriptions,
            quote_subscriptions=self.quote_subscriptions,
            additions=self.additions,
            removals=self.removals,
            planner_version=self.planner_version,
            content_hash=self.content_hash,
            provenance=self.provenance,
            lifecycle="superseded",
            coverage_plan=self.coverage_plan,
            selection_records=self.selection_records,
        )

    def finalize(self) -> "PlannerEpoch":
        """Return a copy with lifecycle='finalized'."""
        return PlannerEpoch(
            epoch_id=self.epoch_id,
            sequence=self.sequence,
            effective_at=self.effective_at,
            candidate_population_version=self.candidate_population_version,
            selected_contracts=self.selected_contracts,
            trade_subscriptions=self.trade_subscriptions,
            quote_subscriptions=self.quote_subscriptions,
            additions=self.additions,
            removals=self.removals,
            planner_version=self.planner_version,
            content_hash=self.content_hash,
            provenance=self.provenance,
            lifecycle="finalized",
            coverage_plan=self.coverage_plan,
            selection_records=self.selection_records,
        )

    @property
    def contract_count(self) -> int:
        return len(self.selected_contracts)

    @property
    def is_noop(self) -> bool:
        """True when no contracts were added or removed vs the previous epoch."""
        return len(self.additions) == 0 and len(self.removals) == 0

    def record(self) -> dict[str, object]:
        """Serialise for persistence alongside UniverseManifest records."""
        return {
            "epoch_id": self.epoch_id,
            "sequence": self.sequence,
            "effective_at": self.effective_at.isoformat(),
            "candidate_population_version": self.candidate_population_version,
            "selected_contracts": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in self.selected_contracts
            ],
            "trade_subscriptions": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in self.trade_subscriptions
            ],
            "quote_subscriptions": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in self.quote_subscriptions
            ],
            "additions": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in self.additions
            ],
            "removals": [
                {"root": c.root, "expiration": c.expiration, "strike": c.strike, "right": c.right}
                for c in self.removals
            ],
            "planner_version": self.planner_version,
            "content_hash": self.content_hash,
            "provenance": self.provenance,
            "lifecycle": self.lifecycle,
            "coverage_plan": self.coverage_plan or {},
            "contract_count": self.contract_count,
            "is_noop": self.is_noop,
        }

    @classmethod
    def from_record(cls, record: dict[str, object]) -> "PlannerEpoch":
        """Reconstruct from a persisted record."""

        def _contracts(key: str) -> tuple[ThetaContract, ...]:
            raw = record.get(key, [])
            if not isinstance(raw, list):
                return ()
            return tuple(
                ThetaContract(
                    str(item["root"]),
                    int(str(item["expiration"])),
                    int(str(item["strike"])),
                    str(item["right"]),
                )
                for item in raw
                if isinstance(item, dict)
            )

        def _parse_datetime(raw: object) -> datetime:
            if isinstance(raw, datetime):
                return raw
            return datetime.fromisoformat(str(raw))

        return cls(
            epoch_id=str(record["epoch_id"]),
            sequence=int(str(record["sequence"])),
            effective_at=_parse_datetime(record["effective_at"]),
            candidate_population_version=str(record["candidate_population_version"]),
            selected_contracts=_contracts("selected_contracts"),
            trade_subscriptions=_contracts("trade_subscriptions"),
            quote_subscriptions=_contracts("quote_subscriptions"),
            additions=_contracts("additions"),
            removals=_contracts("removals"),
            planner_version=str(record["planner_version"]),
            content_hash=str(record["content_hash"]),
            provenance=str(record["provenance"]),
            lifecycle=str(record["lifecycle"]),
            coverage_plan=coverage_plan_raw
            if isinstance(coverage_plan_raw := record.get("coverage_plan"), dict)
            else None,
            selection_records=tuple(
                item
                for item in cast("list[dict[str, object]]", record.get("selection_records", []))
            ),
        )
