from copy import deepcopy

from institutional_signal_engine.phase4b_certify import (
    CERTIFICATION_VERSION,
    SCENARIO_VERSION,
    _certificate,
    scenario_definition,
    scenario_sha256,
    validate_certificate,
)


def test_certificate_is_deterministic_and_expected_fail_closed():
    first = _certificate()
    second = _certificate()
    assert first == second
    assert first["certification_version"] == CERTIFICATION_VERSION
    assert first["scenario_version"] == SCENARIO_VERSION
    assert first["scenario_sha256"] == scenario_sha256()
    assert first["overall"] == "FAIL"
    assert first["orders"] == {"trading_enabled": False, "constructed": 0, "submitted": 0}
    assert validate_certificate(first) == []


def test_scenario_contains_required_accelerated_rth_transitions():
    scenario = scenario_definition()
    assert len(scenario["planner_epochs"]) == 3
    assert scenario["virtual_clock"]["intake_stop"] == "2026-08-10T20:00:00Z"
    assert scenario["provider_acknowledgements"]["rejected"]


def test_certificate_rejects_mutated_trace_or_literal_status():
    certificate = _certificate()
    mutated = deepcopy(certificate)
    mutated["invariants"]["dynamic_admission"]["observed"] = 0
    assert any("invariant-status-mismatch" in error for error in validate_certificate(mutated))

    duplicated = deepcopy(certificate)
    event = next(item for item in duplicated["trace"] if item["kind"] == "normalized_event")
    duplicated["trace"].append(dict(event, record_id="trace-forged-duplicate"))
    assert "duplicate-normalized-event" in validate_certificate(duplicated)


def test_certificate_uses_real_schema_negative_cases():
    certificate = _certificate()
    cases = []
    wrong_type = deepcopy(certificate)
    wrong_type["worktree_dirty"] = "false"
    cases.append(wrong_type)
    missing_nested = deepcopy(certificate)
    del missing_nested["orders"]["submitted"]
    cases.append(missing_nested)
    invalid_constant = deepcopy(certificate)
    invalid_constant["orders"]["constructed"] = 1
    cases.append(invalid_constant)
    malformed_uuid = deepcopy(certificate)
    malformed_uuid["run_id"] = "not-a-uuid"
    cases.append(malformed_uuid)
    malformed_sha = deepcopy(certificate)
    malformed_sha["scenario_sha256"] = "short"
    cases.append(malformed_sha)
    extra = deepcopy(certificate)
    extra["unexpected"] = True
    cases.append(extra)
    assert all(validate_certificate(case) for case in cases)
