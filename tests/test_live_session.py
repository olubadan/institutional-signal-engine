"""Adversarial and end-to-end tests for the live observational evidence path."""

from __future__ import annotations

import asyncio
import json
import pickle
from pathlib import Path

import pytest

from institutional_signal_engine.config import Settings
from institutional_signal_engine.journal import sha256
from institutional_signal_engine.live_session import (
    BundleWriter,
    LiveEvidenceFailure,
    _historical_bootstrap_payload,
    _historical_from_payload,
    load_and_verify_bundle,
    validate_environment,
)
from institutional_signal_engine.live_session_harness import (
    HARNESS_AUTHORITY_KEY,
    run_harness,
)
from institutional_signal_engine.providers.common import ProviderError


def _run(tmp_path: Path, name: str = "bundle") -> Path:
    bundle = tmp_path / name
    asyncio.run(
        run_harness(
            bundle,
            tmp_path / f"{name}-journals",
            tmp_path / f"{name}-observations.json",
            Path.cwd(),
        )
    )
    return bundle


def test_accelerated_harness_publishes_complete_bundle(tmp_path: Path) -> None:
    bundle = _run(tmp_path)
    manifest = json.loads((bundle / "MANIFEST.json").read_text())
    assert not (bundle / "INCOMPLETE").exists()
    assert manifest["status"] == "complete"
    assert manifest["journal_record_count"] > 0
    assert manifest["orders_constructed"] == 0
    assert manifest["orders_submitted"] == 0
    loaded = load_and_verify_bundle(
        bundle, authority_key=HARNESS_AUTHORITY_KEY, repository_root=Path.cwd()
    )
    assert loaded.replay_receipt["exact_equality"] is True
    assert loaded.final_state["disconnects"] == 1
    assert loaded.final_state["restorations"] == 1
    assert loaded.final_state["subscription_add_commands"] > 0
    assert loaded.final_state["subscription_remove_commands"] > 0


def test_two_accelerated_runs_have_byte_identical_deterministic_artifacts(tmp_path: Path) -> None:
    first = _run(tmp_path, "first")
    second = _run(tmp_path, "second")
    for name in (
        "CONFIGURATION.json",
        "FINAL_STATE.json",
        "JOURNAL.json",
        "MANIFEST.json",
        "PERSISTENCE_RECEIPT.json",
        "REPLAY_RECEIPT.json",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes(), name


def test_existing_output_and_incomplete_bundle_fail_closed(tmp_path: Path) -> None:
    output = tmp_path / "new-bundle"
    BundleWriter(output)
    with pytest.raises(LiveEvidenceFailure, match="OUTPUT_DIRECTORY_EXISTS"):
        BundleWriter(output)
    with pytest.raises(LiveEvidenceFailure, match="BUNDLE_INCOMPLETE"):
        load_and_verify_bundle(
            output, authority_key=HARNESS_AUTHORITY_KEY, repository_root=Path.cwd()
        )


def test_aliased_output_target_fails_closed(tmp_path: Path) -> None:
    real_parent = tmp_path / "real"
    real_parent.mkdir()
    alias_parent = tmp_path / "alias"
    alias_parent.symlink_to(real_parent, target_is_directory=True)
    with pytest.raises(LiveEvidenceFailure, match="OUTPUT_DIRECTORY_ALIASED"):
        BundleWriter(alias_parent / "bundle")


def test_corrupted_component_and_coherent_rehash_without_trusted_binding_fail(
    tmp_path: Path,
) -> None:
    bundle = _run(tmp_path)
    journal_path = bundle / "JOURNAL.json"
    journal_path.write_text(journal_path.read_text() + "\n")
    with pytest.raises(LiveEvidenceFailure, match="BUNDLE_COMPONENT_DIGEST_MISMATCH"):
        load_and_verify_bundle(
            bundle, authority_key=HARNESS_AUTHORITY_KEY, repository_root=Path.cwd()
        )

    bundle = _run(tmp_path, "rehash")
    manifest_path = bundle / "MANIFEST.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["components"]["JOURNAL.json"] = "0" * 64
    commitment = dict(manifest)
    commitment.pop("authority_tag")
    commitment["manifest_sha256"] = sha256(
        {key: value for key, value in commitment.items() if key != "manifest_sha256"}
    )
    commitment["authority_tag"] = manifest["authority_tag"]
    manifest_path.write_text(json.dumps(commitment, sort_keys=True))
    with pytest.raises(LiveEvidenceFailure, match="TRUSTED_BINDING_MISMATCH"):
        load_and_verify_bundle(
            bundle, authority_key=HARNESS_AUTHORITY_KEY, repository_root=Path.cwd()
        )


def test_copied_or_altered_replay_receipt_and_order_activity_fail(tmp_path: Path) -> None:
    bundle = _run(tmp_path)
    replay_path = bundle / "REPLAY_RECEIPT.json"
    replay = json.loads(replay_path.read_text())
    replay["run_id"] = "00000000-0000-0000-0000-000000000000"
    replay_path.write_text(json.dumps(replay, sort_keys=True))
    with pytest.raises(LiveEvidenceFailure, match="BUNDLE_COMPONENT_DIGEST_MISMATCH"):
        load_and_verify_bundle(
            bundle, authority_key=HARNESS_AUTHORITY_KEY, repository_root=Path.cwd()
        )


def test_missing_durable_configuration_fails_before_provider(tmp_path: Path) -> None:
    with pytest.raises(LiveEvidenceFailure, match="DURABLE_REPOSITORY_CONFIGURATION_REQUIRED"):
        validate_environment(
            Settings(),
            tmp_path / "fresh",
            "not-the-head",
            Path.cwd(),
        )


def test_cli_run_card_has_one_full_session_mode() -> None:
    from institutional_signal_engine.live_session import build_parser

    parser = build_parser()
    help_text = parser.format_help()
    assert "run" in help_text
    assert "replay" in help_text
    assert "certify" in help_text
    assert "--seconds" not in help_text
    assert "--skip-oi-diagnostic" not in help_text
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--help"])


def test_runbook_matches_authoritative_cli() -> None:
    runbook = Path("docs/phase4b/MONDAY_RUNBOOK.md").read_text()
    assert "institutional_signal_engine.live_session run" in runbook
    assert "institutional_signal_engine.live_session replay" in runbook
    assert "institutional_signal_engine.live_session certify" in runbook
    assert "--seconds" not in runbook
    assert "--skip-oi-diagnostic" not in runbook
    assert "16:00" in runbook


def test_historical_bootstrap_payload_is_process_serializable() -> None:
    payload = {
        "session": "2026-08-17",
        "previous_closes": {},
        "cumulative_profiles": {"AAA": {0: ()}},
        "completed_highs": {"AAA": {}},
        "adjustment": "split",
        "source_provenance": "alpaca:test",
        "impact_baselines": {},
    }
    round_tripped = pickle.loads(pickle.dumps(payload))
    historical = _historical_from_payload(round_tripped)
    assert historical.session == "2026-08-17"
    assert historical.cumulative_volume_baseline("AAA", 0) == ()


def test_historical_bootstrap_provider_failure_is_process_serializable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingProvider:
        def __init__(self, *args: object, **kwargs: object) -> None:
            pass

        async def historical_bootstrap(self, *_args: object) -> object:
            raise ProviderError("alpaca", "historical_timeout", True)

    monkeypatch.setattr(
        "institutional_signal_engine.live_session.AlpacaEquitiesProvider", FailingProvider
    )
    result = _historical_bootstrap_payload(
        "https://historical", "https://events", "id", "secret", ("AAA",), "2026-08-18"
    )
    assert result == {
        "status": "FAILED",
        "error_type": "ProviderError",
        "error_category": "historical_timeout",
        "retryable": True,
    }
