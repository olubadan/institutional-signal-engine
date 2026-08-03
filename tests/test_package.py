"""Baseline package tests."""

from institutional_signal_engine import __version__


def test_package_version_is_explicitly_pre_release() -> None:
    """The baseline must not imply a production-ready release."""
    assert __version__ == "0.0.0"
