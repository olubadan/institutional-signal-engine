from institutional_signal_engine.ignition import evaluate_ignition


def test_ignition_passes_only_when_every_check_passes():
    assert evaluate_ignition({"commit": True, "trading_disabled": True}).passed
    result = evaluate_ignition({"commit": True, "trading_disabled": False})
    assert not result.passed
    assert result.reasons == ("trading_disabled",)


def test_ignition_result_is_sanitized_and_orders_are_zero():
    payload = evaluate_ignition({"secret_present": True}).as_dict()
    assert payload["trading_enabled"] is False
    assert payload["orders_constructed"] == 0
    assert payload["orders_submitted"] == 0
