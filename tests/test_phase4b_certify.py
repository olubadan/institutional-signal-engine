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
