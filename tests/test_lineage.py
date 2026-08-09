from datetime import date
from uuid import uuid4

from test_alpaca_contracts import contract

from institutional_signal_engine.contract_mapping import map_alpaca_contract
from institutional_signal_engine.impact_coverage import (
    build_coverage_plan,
    build_pilot_coverage_candidates,
)
from institutional_signal_engine.lineage import (
    RunUniverseFinalization,
    build_contract_transitions,
    replay_finalization,
)
from institutional_signal_engine.providers.thetadata import ThetaContract
from institutional_signal_engine.universe import AlpacaContractSelector, UniverseSelection


def test_later_expiration_and_duplicate_have_explicit_terminal_reasons():
    first = map_alpaca_contract(contract())
    duplicate = map_alpaca_contract(contract(id="contract-duplicate"))
    later = map_alpaca_contract(
        contract(
            id="contract-later",
            symbol="AAPL  260821C00310000",
            expiration_date="2026-08-21",
        )
    )
    _selections, exclusions = AlpacaContractSelector().coarse_select(
        (first, duplicate, later),
        {"AAPL": 310},
        date(2026, 7, 31),
        symbols=("AAPL",),
        max_per_symbol=10,
    )
    reasons = {str(item["reason"]) for item in exclusions}
    assert "DUPLICATE_CANONICAL_IDENTITY" in reasons
    assert "COARSE_SHORTLIST_EXPIRATION_EXCLUDED" in reasons
    assert all("canonical_identity" in item for item in exclusions)


def test_finalization_replay_preserves_ordered_allocation_and_run_id():
    run_id = uuid4()
    result = map_alpaca_contract(contract())
    selection = UniverseSelection(
        "AAPL",
        True,
        20260807,
        (ThetaContract("AAPL", 20260807, 310000, "C"),),
        (),
        (),
        (),
        (
            {
                "provider_symbol": "AAPL  260807C00310000",
                "root": "AAPL",
                "expiration": 20260807,
                "strike": 310000,
                "right": "C",
                "bid_price": "4",
                "ask_price": "4.01",
                "underlying_price": "310",
            },
        ),
    )
    candidates = build_pilot_coverage_candidates((selection,), frozenset({"AAPL"}))
    plan = build_coverage_plan(candidates)
    requests = (
        {
            "root": "AAPL",
            "expiration": 20260807,
            "strike": 310000,
            "right": "C",
            "request_type": "TRADE",
            "acknowledged": True,
        },
    )
    transitions = build_contract_transitions(
        run_id,
        (result,),
        (selection,),
        (),
        (
            {
                "root": "AAPL",
                "expiration": 20260807,
                "strike": 310000,
                "right": "C",
                "rejection_reasons": (),
            },
        ),
        plan,
        requests,
    )
    finalization = RunUniverseFinalization(
        run_id,
        date(2026, 8, 7),
        transitions,
        plan.as_dict(),
        ({"root": "AAPL", "expiration": 20260807, "strike": 310000, "right": "C"},),
        requests,
        (),
        requests,
        "test",
        {"coverage": "v1"},
    ).record()
    assert finalization["run_id"] == str(run_id)
    assert replay_finalization(finalization) == finalization
    assert any(item["state"] == "ACKNOWLEDGED" for item in transitions)


def test_u_x_is_never_selected_by_final_allocation():
    candidate = build_pilot_coverage_candidates(
        (
            UniverseSelection(
                "AAPL", True, 20260807, (ThetaContract("AAPL", 20260807, 310000, "C"),), (), (), ()
            ),
        ),
        frozenset(),
    )[0]
    assert candidate.coverage_set == "U_R"
    assert all(
        item.coverage_set != "PROVABLY_EXCLUDABLE"
        for item in build_coverage_plan((candidate,)).selected
    )
