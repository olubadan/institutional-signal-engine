"""Authority-graph and adversarial tests for Phase 4B certification."""

from __future__ import annotations

import ast
import inspect
import json
import os
from copy import deepcopy
from dataclasses import fields, replace
from pathlib import Path
from typing import Any, cast

import pytest

import institutional_signal_engine.phase4b_certify as certification
from institutional_signal_engine.journal import (
    JOURNAL_KIND_CONFIGURATION,
    JOURNAL_KIND_EPOCH_CREATED,
    JOURNAL_KIND_EVENT_ACCEPTED,
    JOURNAL_KIND_EVENT_REJECTED,
    AcceptanceContract,
    InMemoryJournalRepository,
    Journal,
    JournalFailure,
    PersistenceReceipt,
    ReplayReceipt,
    canonical_json,
    sha256,
    sha256_bytes,
    verify_complete,
)
from institutional_signal_engine.phase4b_certify import (
    CertificationArtifacts,
    CertificationOutputPaths,
    generate_phase4b_certification,
    validate_certificate_schema,
)

RawJournal = dict[str, Any]


def _paths(directory: Path) -> CertificationOutputPaths:
    return CertificationOutputPaths(
        certificate=directory / "CERTIFICATE.json",
        journal=directory / "JOURNAL.json",
        persistence_receipt=directory / "PERSISTENCE_RECEIPT.json",
        replay_receipt=directory / "REPLAY_RECEIPT.json",
    )


@pytest.fixture()
def generated(tmp_path: Path) -> tuple[CertificationArtifacts, dict[str, Any]]:
    artifacts = generate_phase4b_certification(_paths(tmp_path))
    certificate = cast(
        dict[str, Any], json.loads(tmp_path.joinpath("CERTIFICATE.json").read_text())
    )
    return artifacts, certificate


def _record(raw: RawJournal, kind: str, occurrence: int = 0) -> dict[str, Any]:
    return [record for record in raw["records"] if record["kind"] == kind][occurrence]


def _rehash(raw: RawJournal) -> None:
    records = cast(list[dict[str, Any]], raw["records"])
    old_to_new: dict[int, int] = {}
    for new_sequence, record in enumerate(records):
        old_to_new.setdefault(int(record["sequence"]), new_sequence)
    previous_digest = "0" * 64
    for sequence, record in enumerate(records):
        old_parent = record.get("parent_sequence")
        record["sequence"] = sequence
        if old_parent is not None:
            record["parent_sequence"] = old_to_new.get(int(old_parent), max(0, sequence - 1))
            parent = records[int(record["parent_sequence"])]
            record["cause_operation_id"] = parent["operation_id"]
        else:
            record["cause_operation_id"] = None
        record["payload_digest"] = sha256(record["payload"])
        record["previous_record_digest"] = previous_digest
        digest_input = {key: value for key, value in record.items() if key != "record_digest"}
        record["record_digest"] = sha256(digest_input)
        previous_digest = record["record_digest"]
    commitment = {
        "run_id": raw["run_id"],
        "record_count": len(records),
        "terminal_sequence": records[-1]["sequence"],
        "terminal_digest": records[-1]["record_digest"],
    }
    raw["seal"] = {**commitment, "seal_digest": sha256(commitment)}


def _mutated_journal(serialized: str, mutation: Any, *, rehash: bool = False) -> str:
    raw = cast(RawJournal, json.loads(serialized))
    mutation(raw)
    if rehash:
        _rehash(raw)
    return canonical_json(raw)


def test_authoritative_generation_binds_every_authority(
    generated: tuple[CertificationArtifacts, dict[str, Any]],
) -> None:
    artifacts, certificate = generated
    assert certificate["overall"] == "PASS"
    assert certificate["failed_invariants"] == []
    assert (
        certificate["acceptance_contract"]["canonical_sha256"]
        == certification.CONTRACT_CANONICAL_SHA256
    )
    assert certificate["deterministic_scenario"] == {
        "version": certification.SCENARIO_VERSION,
        "canonical_sha256": certification.SCENARIO_CANONICAL_SHA256,
    }
    observed = certificate["observed"]
    assert observed["run_id"] == str(certification.RUN_ID)
    assert observed["journal"]["canonical_byte_sha256"] == artifacts.journal_sha256
    assert observed["persistence"]["persistence_identity"].endswith(artifacts.journal_sha256)
    assert observed["persistence"]["receipt_digest"]
    assert observed["replay"]["receipt_digest"]
    assert observed["replay"]["reconstructed_root_digest"] == observed["journal"]["root_digest"]
    assert observed["replay"]["exact_equality"] is True
    assert (
        observed["replay"]["replayed_projection_digest"]
        == certificate["observed_projection_sha256"]
    )
    assert observed["orders"] == {"trading_enabled": False, "constructed": 0, "submitted": 0}
    commitment = dict(certificate)
    digest = commitment.pop("certificate_canonical_sha256")
    assert digest == sha256(commitment) == artifacts.certificate_commitment_sha256
    assert validate_certificate_schema(certificate) == []


def test_two_independent_executions_are_byte_identical(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    one = generate_phase4b_certification(_paths(first))
    two = generate_phase4b_certification(_paths(second))
    assert one.journal_sha256 == two.journal_sha256
    assert one.persistence_receipt_sha256 == two.persistence_receipt_sha256
    assert one.replay_receipt_sha256 == two.replay_receipt_sha256
    assert one.certificate_sha256 == two.certificate_sha256
    for name in (
        "JOURNAL.json",
        "PERSISTENCE_RECEIPT.json",
        "REPLAY_RECEIPT.json",
        "CERTIFICATE.json",
    ):
        assert first.joinpath(name).read_bytes() == second.joinpath(name).read_bytes()


def test_public_authoritative_interface_is_output_paths_only() -> None:
    assert list(inspect.signature(generate_phase4b_certification).parameters) == ["output_paths"]
    assert [field.name for field in fields(CertificationOutputPaths)] == [
        "certificate",
        "journal",
        "persistence_receipt",
        "replay_receipt",
    ]
    assert certification.__all__ == [
        "CertificationArtifacts",
        "CertificationOutputPaths",
        "generate_phase4b_certification",
        "main",
        "validate_certificate_schema",
    ]
    forbidden = {
        "Composition",
        "VerifiedEvidencePackage",
        "acceptance_projection",
        "certify_repository_backed",
        "execute_composition",
        "journal_projection",
        "load_acceptance_contract",
        "replay_persisted",
        "write_artifacts",
        "_issue",
    }
    assert forbidden.isdisjoint(vars(certification))


@pytest.mark.parametrize(
    "semantic_value",
    [
        object(),
        InMemoryJournalRepository(),
        AcceptanceContract,
        Journal,
        PersistenceReceipt,
        ReplayReceipt,
        {"overall": "PASS"},
    ],
)
def test_no_semantic_object_can_enter_authoritative_api(
    tmp_path: Path, semantic_value: object
) -> None:
    with pytest.raises(TypeError):
        generate_phase4b_certification(_paths(tmp_path), semantic_value)  # type: ignore[call-arg]
    assert not tmp_path.joinpath("CERTIFICATE.json").exists()


def test_low_level_lookalikes_and_object_mutation_have_no_authoritative_consumer(
    tmp_path: Path,
) -> None:
    class FormerCapabilityLookalike:
        pass

    forged = object.__new__(FormerCapabilityLookalike)
    object.__setattr__(forged, "_authority", object())
    object.__setattr__(forged, "overall", "PASS")
    copied = replace(
        CertificationArtifacts(_paths(tmp_path), "0" * 64, "0" * 64, "0" * 64, "0" * 64, "0" * 64),
        certificate_sha256="f" * 64,
    )
    assert forged.overall == "PASS"
    assert copied.certificate_sha256 == "f" * 64
    with pytest.raises(TypeError):
        generate_phase4b_certification(_paths(tmp_path), forged)  # type: ignore[call-arg]
    with pytest.raises(TypeError):
        generate_phase4b_certification(_paths(tmp_path), copied)  # type: ignore[call-arg]


def test_manual_schema_valid_certificate_is_shape_only_and_has_no_consumer(
    generated: tuple[CertificationArtifacts, dict[str, Any]],
) -> None:
    _, certificate = generated
    manual = json.loads(json.dumps(certificate))
    assert validate_certificate_schema(manual) == []
    assert manual["overall"] == "PASS"
    assert list(inspect.signature(validate_certificate_schema).parameters) == ["value"]
    assert (
        "NON_AUTHORITATIVE" in validate_certificate_schema.__doc__.upper()
        or "NEVER OPERATIONAL" in validate_certificate_schema.__doc__.upper()
    )
    assert not any("certificate" in name and "load" in name for name in certification.__all__)


def test_exactly_one_production_function_constructs_overall_pass() -> None:
    producers: list[str] = []
    for path in Path("src").rglob("*.py"):
        module = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(module):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            if any(
                isinstance(item, ast.Constant) and item.value == "PASS" for item in ast.walk(node)
            ):
                producers.append(f"{path.name}:{node.name}")
    assert producers == ["phase4b_certify.py:generate_phase4b_certification"]


def test_no_environment_or_test_import_can_select_certification_semantics() -> None:
    source = inspect.getsource(certification)
    tree = ast.parse(source)
    assert "os.environ" not in source
    assert "getenv(" not in source
    assert "Settings.from_env" not in source
    assert not any(
        isinstance(node, (ast.Import, ast.ImportFrom))
        and any(alias.name.startswith("tests") for alias in node.names)
        for node in ast.walk(tree)
    )


def test_environment_and_cwd_do_not_select_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    work = tmp_path / "work"
    work.mkdir()
    fake = work / "phase4b_acceptance_contract.json"
    fake.write_text('{"contract_version":"ATTACK"}', encoding="utf-8")
    monkeypatch.chdir(work)
    monkeypatch.setenv("PHASE4B_CONTRACT_PATH", str(fake))
    monkeypatch.setenv("RUNTIME_ENV_FILE", str(fake))
    artifacts = generate_phase4b_certification(_paths(tmp_path / "out"))
    assert (
        artifacts.journal_sha256
        == "932ce1afbbd5bd6a84393962fdd9ad06ad1c1b095d789820a5019648b33244b8"
    )


def test_missing_contract_resource_fails_stably(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def missing(_: str) -> object:
        raise FileNotFoundError

    monkeypatch.setattr(certification.resources, "files", missing)
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == "CONTRACT_RESOURCE_MISSING"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value + b"\n",
        lambda value: value.replace(b'"orders_constructed": 0', b'"orders_constructed": 1'),
        lambda value: value.replace(
            b"PHASE4B_ACCEPTANCE_CONTRACT_V3", b"OTHER_ACCEPTANCE_CONTRACT___"
        ),
        lambda value: value.replace(b'"A:20260821:10000:C"', b'"X:20260821:10000:C"', 1),
    ],
)
def test_modified_contract_bytes_fail_before_deserialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: Any
) -> None:
    original = certification._contract_resource_bytes()
    monkeypatch.setattr(certification, "_contract_resource_bytes", lambda: mutation(original))
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == "CONTRACT_DIGEST_MISMATCH"


def test_incorrect_pinned_contract_digest_fails_stably(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(certification, "CONTRACT_CANONICAL_SHA256", "0" * 64)
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == "CONTRACT_DIGEST_MISMATCH"


def test_malformed_digest_pinned_contract_fails_deserialization(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    malformed = b"{"
    monkeypatch.setattr(certification, "CONTRACT_CANONICAL_SHA256", sha256_bytes("{"))
    monkeypatch.setattr(certification, "_contract_resource_bytes", lambda: malformed)
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == "CONTRACT_DESERIALIZATION_FAILED"


def _drop_root(raw: dict[str, Any]) -> None:
    raw.pop("purpose")


def _add_root(raw: dict[str, Any]) -> None:
    raw["alternate_authority"] = True


def _drop_expected(raw: dict[str, Any]) -> None:
    raw["expected"].pop("accepted_events")


def _duplicate_expected_event(raw: dict[str, Any]) -> None:
    raw["expected"]["accepted_events"].append(deepcopy(raw["expected"]["accepted_events"][0]))


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (_drop_root, "CONTRACT_VALIDATION_FAILED"),
        (_add_root, "CONTRACT_VALIDATION_FAILED"),
        (_drop_expected, "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw.update(contract_version="OTHER"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw.update(certificate_version="OTHER"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw.update(purpose=1), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw.update(allowed_record_kinds="all"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw["allowed_record_kinds"].pop(), "CONTRACT_RECORD_VOCABULARY_INVALID"),
        (lambda raw: raw.update(allowed_lifecycle_order=[1]), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw.update(required_invariants="run_binding"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw.update(required_invariants=[]), "CONTRACT_VALIDATION_FAILED"),
        (
            lambda raw: raw.update(external_build_envelope_excluded=[1]),
            "CONTRACT_VALIDATION_FAILED",
        ),
        (lambda raw: raw["safety"].update(trading_enabled=True), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw["expected"].update(run_id="not-a-uuid"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw["expected"].update(rth_start="not-a-time"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw["expected"].update(epoch_memberships="A"), "CONTRACT_VALIDATION_FAILED"),
        (lambda raw: raw["expected"].update(clock_boundaries=[1]), "CONTRACT_VALIDATION_FAILED"),
        (
            lambda raw: raw["expected"].update(restoration_membership=[1]),
            "CONTRACT_VALIDATION_FAILED",
        ),
        (lambda raw: raw["expected"].update(accepted_events="all"), "CONTRACT_VALIDATION_FAILED"),
        (
            lambda raw: raw["expected"]["accepted_events"][0].pop("channel"),
            "CONTRACT_VALIDATION_FAILED",
        ),
        (
            lambda raw: raw["expected"]["accepted_events"][0].update(epoch_sequence="1"),
            "CONTRACT_VALIDATION_FAILED",
        ),
        (
            lambda raw: raw["expected"]["accepted_events"][0].update(event_id="not-a-uuid"),
            "CONTRACT_VALIDATION_FAILED",
        ),
        (_duplicate_expected_event, "CONTRACT_VALIDATION_FAILED"),
    ],
)
def test_digest_pinned_contract_is_fully_validated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: Any,
    expected_code: str,
) -> None:
    original_reader = certification._contract_resource_bytes
    raw = cast(
        dict[str, Any],
        json.loads(original_reader()),
    )
    mutation(raw)
    replacement = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    monkeypatch.setattr(
        certification,
        "CONTRACT_CANONICAL_SHA256",
        sha256_bytes(replacement.decode()),
    )
    monkeypatch.setattr(
        certification,
        "_contract_resource_bytes",
        lambda: replacement,
    )
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == expected_code


def test_incorrect_scenario_digest_fails_before_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(certification, "SCENARIO_CANONICAL_SHA256", "0" * 64)
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == "SCENARIO_DIGEST_MISMATCH"


@pytest.mark.parametrize(
    "paths,code",
    [
        (
            lambda root: CertificationOutputPaths(
                root / "same", root / "same", root / "p", root / "r"
            ),
            "OUTPUT_PATHS_NOT_DISTINCT",
        ),
        (
            lambda root: CertificationOutputPaths(
                root / "a" / "c", root / "b" / "j", root / "a" / "p", root / "a" / "r"
            ),
            "OUTPUT_PATHS_INVALID",
        ),
    ],
)
def test_invalid_output_locations_fail_before_execution(
    tmp_path: Path, paths: Any, code: str
) -> None:
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(paths(tmp_path))
    assert failure.value.code == code
    assert not tuple(tmp_path.rglob("CERTIFICATE.json"))


def test_existing_or_symlink_output_target_is_never_overwritten(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    paths.journal.write_text("sentinel", encoding="utf-8")
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(paths)
    assert failure.value.code == "OUTPUT_TARGET_EXISTS"
    assert paths.journal.read_text(encoding="utf-8") == "sentinel"
    paths.journal.unlink()
    target = tmp_path / "target"
    target.write_text("sentinel", encoding="utf-8")
    paths.journal.symlink_to(target)
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(paths)
    assert failure.value.code == "OUTPUT_TARGET_EXISTS"
    assert target.read_text(encoding="utf-8") == "sentinel"


def test_publication_failure_leaves_no_certificate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_replace = Path.replace
    calls = 0

    def fail_second(source: Path, target: Path) -> Path:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("injected")
        return original_replace(source, target)

    monkeypatch.setattr(Path, "replace", fail_second)
    with pytest.raises(JournalFailure) as failure:
        generate_phase4b_certification(_paths(tmp_path))
    assert failure.value.code == "ARTIFACT_PUBLICATION_FAILED"
    assert not tmp_path.joinpath("CERTIFICATE.json").exists()
    assert not any(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        (lambda raw: raw.update(seal=None), "UNSEALED_JOURNAL"),
        (
            lambda raw: raw["records"].append(dict(raw["records"][-1])),
            "SEQUENCE_INVALID",
        ),
        (
            lambda raw: raw["records"].insert(0, raw["records"].pop()),
            "SEQUENCE_INVALID",
        ),
        (
            lambda raw: _record(raw, JOURNAL_KIND_CONFIGURATION)["payload"].update(
                acceptance_contract_version="OTHER"
            ),
            "JOURNAL_CONTRACT_VERSION_MISMATCH",
        ),
        (
            lambda raw: _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)["payload"].update(
                membership=["A:20260821:10000:C"]
            ),
            "EPOCH_MEMBERSHIP_INVALID",
        ),
        (
            lambda raw: _record(raw, JOURNAL_KIND_EVENT_ACCEPTED)["payload"].update(
                event_id="4b000000-0000-4000-8000-000000000099"
            ),
            "UNEXPECTED_ACCEPTED_EVENT",
        ),
        (
            lambda raw: _record(raw, JOURNAL_KIND_EVENT_REJECTED)["payload"].update(
                event_id="4b000000-0000-4000-8000-000000000099"
            ),
            "UNEXPECTED_REJECTED_EVENT",
        ),
        (
            lambda raw: raw["records"].append(
                {
                    **dict(raw["records"][-1]),
                    "kind": JOURNAL_KIND_EVENT_ACCEPTED,
                    "operation_id": "after-finalization",
                }
            ),
            "RECORD_AFTER_FINALIZATION",
        ),
    ],
)
def test_low_level_journal_attacks_are_falsified_but_never_authoritative(
    generated: tuple[CertificationArtifacts, dict[str, Any]],
    mutation: Any,
    expected_code: str,
) -> None:
    artifacts, _ = generated
    serialized = artifacts.output_paths.journal.read_text(encoding="utf-8")
    rehash = expected_code not in {"UNSEALED_JOURNAL", "SEQUENCE_INVALID"}
    attacked = _mutated_journal(serialized, mutation, rehash=rehash)
    contract = certification._load_authoritative_contract().value
    with pytest.raises(JournalFailure) as failure:
        verify_complete(Journal.deserialize(attacked), contract)
    assert failure.value.code == expected_code
    with pytest.raises(TypeError):
        generate_phase4b_certification(artifacts.output_paths, attacked)  # type: ignore[call-arg]


def test_coherently_constructed_low_level_package_remains_non_authoritative(
    generated: tuple[CertificationArtifacts, dict[str, Any]], tmp_path: Path
) -> None:
    artifacts, _ = generated
    contract = certification._load_authoritative_contract().value
    verified = verify_complete(
        Journal.deserialize(artifacts.output_paths.journal.read_text(encoding="utf-8")), contract
    )
    repository = InMemoryJournalRepository()
    identity = repository.save(verified.run_id, verified.canonical_serialization)
    persistence = PersistenceReceipt.create(contract, verified, identity)
    projection_digest = "a" * 64
    replay = ReplayReceipt.from_values(
        contract_version=contract.contract_version,
        run_id=verified.run_id,
        persistence_identity=identity,
        original_root_digest=verified.root_digest,
        reconstructed_root_digest=verified.root_digest,
        original_byte_sha256=sha256_bytes(verified.canonical_serialization),
        reconstructed_byte_sha256=sha256_bytes(verified.canonical_serialization),
        original_record_count=verified.seal.record_count,
        reconstructed_record_count=verified.seal.record_count,
        original_projection_digest=projection_digest,
        replayed_projection_digest=projection_digest,
        exact_equality=True,
    )
    assert persistence.persistence_completed is True
    assert replay.exact_equality is True
    fresh_paths = _paths(tmp_path / "fresh")
    for value in (contract, verified, persistence, replay, repository):
        with pytest.raises(TypeError):
            generate_phase4b_certification(fresh_paths, value)  # type: ignore[call-arg]


def test_copied_or_mutated_certificate_does_not_become_authoritative(
    generated: tuple[CertificationArtifacts, dict[str, Any]], tmp_path: Path
) -> None:
    _, certificate = generated
    mutations = (
        lambda value: value.pop("observed_projection_sha256"),
        lambda value: value["observed"]["orders"].update(constructed=1),
        lambda value: value["acceptance_contract"].update(contract_version="OTHER"),
        lambda value: value["deterministic_scenario"].update(canonical_sha256="0" * 64),
        lambda value: value.update(certificate_canonical_sha256="0" * 64),
    )
    for index, mutation in enumerate(mutations):
        candidate = json.loads(json.dumps(certificate))
        mutation(candidate)
        # Shape validation may reject a mutation, but success could only mean shape.
        validate_certificate_schema(candidate)
        with pytest.raises(TypeError):
            generate_phase4b_certification(_paths(tmp_path / str(index)), candidate)  # type: ignore[call-arg]


def test_build_envelope_is_absent_from_runtime_certificate(
    generated: tuple[CertificationArtifacts, dict[str, Any]],
) -> None:
    _, certificate = generated
    serialized = canonical_json(certificate).lower()
    for forbidden in ("git_head", "worktree", "github_ci", "pull_request", "pr_state"):
        assert forbidden not in serialized


def test_contract_and_schema_resources_are_package_relative_and_checked_in() -> None:
    root = Path(inspect.getfile(certification)).parent
    contract = root / certification.CONTRACT_RESOURCE
    schema = root / certification.SCHEMA_RESOURCE
    assert contract.is_file() and not contract.is_symlink()
    assert schema.is_file() and not schema.is_symlink()
    assert (
        sha256_bytes(contract.read_text(encoding="utf-8"))
        == certification.CONTRACT_CANONICAL_SHA256
    )
    assert "docs/phase4b" not in inspect.getsource(certification)


def test_cli_rejects_alternate_semantic_flags_without_artifacts(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as failure:
        certification.main(["--contract", str(tmp_path / "contract.json")])
    assert failure.value.code == 2
    assert not tuple(tmp_path.iterdir())


def test_artifact_hashes_are_exact_bytes(
    generated: tuple[CertificationArtifacts, dict[str, Any]],
) -> None:
    artifacts, _ = generated
    assert artifacts.journal_sha256 == sha256_bytes(artifacts.output_paths.journal.read_text())
    assert artifacts.persistence_receipt_sha256 == sha256_bytes(
        artifacts.output_paths.persistence_receipt.read_text()
    )
    assert artifacts.replay_receipt_sha256 == sha256_bytes(
        artifacts.output_paths.replay_receipt.read_text()
    )
    assert artifacts.certificate_sha256 == sha256_bytes(
        artifacts.output_paths.certificate.read_text()
    )


def test_no_live_provider_or_order_path_is_reachable_from_certification_module() -> None:
    source = inspect.getsource(certification)
    assert "from .providers.alpaca" not in source
    assert "Alpaca" not in source
    assert "submit_order" not in source
    assert os.environ.get("TRADING_ENABLED") is None or "TRADING_ENABLED" not in source
