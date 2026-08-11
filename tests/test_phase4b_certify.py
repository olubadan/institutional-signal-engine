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
    # Key fields must be deterministic
    assert first["certification_version"] == second["certification_version"]
    assert first["scenario_version"] == second["scenario_version"]
    assert first["scenario_sha256"] == second["scenario_sha256"]
    assert first["configuration_versions"] == second["configuration_versions"]
    assert first["invariants"].keys() == second["invariants"].keys()
    for key in first["invariants"]:
        assert first["invariants"][key]["expected"] == second["invariants"][key]["expected"], key
        assert first["invariants"][key]["status"] == second["invariants"][key]["status"], key
    assert first["certification_version"] == CERTIFICATION_VERSION
    assert first["scenario_version"] == SCENARIO_VERSION
    assert first["scenario_sha256"] == scenario_sha256()
    assert first["orders"] == {"trading_enabled": False, "constructed": 0, "submitted": 0}


def test_scenario_contains_required_accelerated_rth_transitions():
    scenario = scenario_definition()
    assert len(scenario["planner_epochs"]) == 3
    assert scenario["virtual_clock"]["intake_stop"] == "2026-08-10T20:00:00Z"
    assert scenario["provider_acknowledgements"]["rejected"]


def test_certificate_rejects_mutated_trace_or_literal_status():
    certificate = _certificate()
    # Mutate observed to False to create a status mismatch
    mutated = deepcopy(certificate)
    mutated["invariants"]["dynamic_admission"]["observed"] = False
    errors = validate_certificate(mutated)
    # Must detect either schema rejection or status-mismatch
    assert len(errors) > 0


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
