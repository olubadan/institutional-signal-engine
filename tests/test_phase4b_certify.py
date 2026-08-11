"""Genuine adversarial tests for CDelta and Omega = (J, P, R)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from typing import Any, cast

import pytest

from institutional_signal_engine.journal import (
    JOURNAL_KIND_CLOCK_ADVANCED,
    JOURNAL_KIND_CONFIGURATION,
    JOURNAL_KIND_DISCOVERY_COMPLETE,
    JOURNAL_KIND_DISCOVERY_START,
    JOURNAL_KIND_EPOCH_ACTIVATED,
    JOURNAL_KIND_EPOCH_CREATED,
    JOURNAL_KIND_EPOCH_RESTORED,
    JOURNAL_KIND_EVENT_ACCEPTED,
    JOURNAL_KIND_REEVALUATION_START,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    AcceptanceContract,
    Journal,
    JournalFailure,
    PersistenceReceipt,
    ReplayReceipt,
    canonical_json,
    sha256,
    verify_complete,
)
from institutional_signal_engine.phase4b_certify import (
    Composition,
    VerifiedEvidencePackage,
    acceptance_projection,
    execute_composition,
    replay_persisted,
    validate_certificate,
)

RawJournal = dict[str, Any]


@pytest.fixture(scope="module")
def composition() -> Composition:
    return asyncio.run(execute_composition())


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


def _mutate_journal(
    serialized: str, mutation: Callable[[RawJournal], None], *, rehash: bool = False
) -> str:
    raw = cast(RawJournal, json.loads(serialized))
    mutation(raw)
    if rehash:
        _rehash(raw)
    return canonical_json(raw)


def _journal_failure(contract: AcceptanceContract, serialized: str) -> str:
    try:
        verify_complete(Journal.deserialize(serialized), contract)
    except JournalFailure as exc:
        return exc.code
    pytest.fail("mutated J unexpectedly passed production verification")


def _projection_failure(
    contract: AcceptanceContract,
    journal: object,
    persistence: PersistenceReceipt,
    replay: ReplayReceipt,
) -> str:
    try:
        acceptance_projection(
            contract,
            VerifiedEvidencePackage(cast(Any, journal), persistence, replay),
        )
    except JournalFailure as exc:
        return exc.code
    pytest.fail("mutated evidence unexpectedly passed production projection")


def _redigest_persistence(receipt: PersistenceReceipt) -> PersistenceReceipt:
    empty = replace(receipt, receipt_digest="")
    return replace(empty, receipt_digest=sha256(empty.commitment()))


def _redigest_replay(receipt: ReplayReceipt) -> ReplayReceipt:
    empty = replace(receipt, receipt_digest="")
    return replace(empty, receipt_digest=sha256(empty.commitment()))


def test_positive_evidence_bundle_and_projection_are_exact(composition: Composition) -> None:
    certificate = acceptance_projection(composition.contract, composition.evidence)
    assert composition.evidence.journal.records[-1].kind == JOURNAL_KIND_SESSION_FINALIZED
    assert composition.evidence.journal.seal.record_count == len(
        composition.evidence.journal.records
    )
    assert composition.reconstructed is not composition.evidence.journal
    assert composition.evidence.persistence.persistence_completed is True
    assert composition.evidence.replay.exact_equality is True
    assert (
        PersistenceReceipt.deserialize(composition.evidence.persistence.serialize())
        == composition.evidence.persistence
    )
    assert ReplayReceipt.deserialize(composition.evidence.replay.serialize()) == (
        composition.evidence.replay
    )
    assert certificate["overall"] == "PASS"
    assert validate_certificate(certificate) == []


def test_j_contains_no_circular_persistence_or_replay_claim(composition: Composition) -> None:
    kinds = {record.kind for record in composition.evidence.journal.records}
    assert "journal.persisted" not in kinds
    assert "replay.verified" not in kinds


def test_deleted_required_record(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        raw["records"].remove(_record(raw, JOURNAL_KIND_CONFIGURATION))

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "MISSING_REQUIRED_RECORD"
    )


def test_reordered_records(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        raw["records"][5], raw["records"][6] = raw["records"][6], raw["records"][5]

    assert _journal_failure(composition.contract, _mutate_journal(serialized, mutate)) == (
        "SEQUENCE_INVALID"
    )


def test_altered_payload_without_digest(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_CONFIGURATION)["payload"]["ack_timeout"] = 999

    assert _journal_failure(composition.contract, _mutate_journal(serialized, mutate)) == (
        "PAYLOAD_DIGEST_MISMATCH"
    )


def test_missing_causal_parent(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        record = _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)
        record["parent_sequence"] = None

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "CAUSAL_PARENT_WRONG"
    )


def test_wrong_same_kind_causal_parent(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        child = _record(raw, JOURNAL_KIND_DISCOVERY_COMPLETE, 1)
        wrong = _record(raw, JOURNAL_KIND_DISCOVERY_START, 0)
        child["parent_sequence"] = wrong["sequence"]
        child["cause_operation_id"] = wrong["operation_id"]

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "DISCOVERY_CORRELATION_INVALID"
    )


def test_fabricated_unknown_record_with_recomputed_hashes(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        stop_index = next(
            index for index, item in enumerate(raw["records"]) if item["kind"] == "intake.stopped"
        )
        clone = deepcopy(raw["records"][stop_index - 1])
        clone.update(
            kind="fabricated.material",
            operation_id="fabricated-operation",
            correlation_id="fabricated-correlation",
        )
        raw["records"].insert(stop_index, clone)

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "UNKNOWN_RECORD_KIND"
    )


def test_fabricated_accepted_event_with_recomputed_hashes(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        stop_index = next(
            index for index, item in enumerate(raw["records"]) if item["kind"] == "intake.stopped"
        )
        clone = deepcopy(_record(raw, JOURNAL_KIND_EVENT_ACCEPTED, -1))
        clone["payload"]["event_id"] = "4b000000-0000-4000-8000-000000000999"
        clone["operation_id"] = "event-4b000000-0000-4000-8000-000000000999"
        clone["correlation_id"] = "4b000000-0000-4000-8000-000000000999"
        raw["records"].insert(stop_index, clone)

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "UNEXPECTED_ACCEPTED_EVENT"
    )


def test_duplicate_event(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        stop_index = next(
            index for index, item in enumerate(raw["records"]) if item["kind"] == "intake.stopped"
        )
        clone = deepcopy(_record(raw, JOURNAL_KIND_EVENT_ACCEPTED, -1))
        clone["operation_id"] = "duplicate-event-operation"
        raw["records"].insert(stop_index, clone)

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "DUPLICATE_EVENT"
    )


def test_changed_journal_run_identity(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        changed = "4b000000-0000-4000-8000-000000000099"
        raw["run_id"] = changed
        for record in raw["records"]:
            record["run_id"] = changed

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "CONTRACT_RUN_BINDING_MISMATCH"
    )


def test_mismatched_run_id_across_j_p_r(composition: Composition) -> None:
    changed = replace(
        composition.evidence.persistence,
        run_id=type(composition.evidence.persistence.run_id)(
            "4b000000-0000-4000-8000-000000000099"
        ),
    )
    changed = _redigest_persistence(changed)
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            changed,
            composition.evidence.replay,
        )
        == "EVIDENCE_RUN_ID_MISMATCH"
    )


def test_incorrect_restoration_membership(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_RESTORED)["payload"]["membership"] = ["A:20260821:10000:C"]

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "RESTORATION_MEMBERSHIP_INVALID"
    )


def test_incorrect_scheduled_boundary_in_contract(composition: Composition) -> None:
    changed = replace(
        composition.contract,
        expected_clock_boundaries=(
            "2026-08-11T13:36:00+00:00",
            "2026-08-11T13:40:00+00:00",
        ),
    )
    assert (
        _projection_failure(
            changed,
            composition.evidence.journal,
            composition.evidence.persistence,
            composition.evidence.replay,
        )
        == "SCHEDULED_BOUNDARY_INVALID"
    )


@pytest.mark.parametrize(
    ("field", "value"),
    [("reconstructed_record_count", 999), ("reconstructed_root_digest", "f" * 64)],
)
def test_fabricated_replay_counts_or_digests(
    composition: Composition, field: str, value: object
) -> None:
    changed = _redigest_replay(replace(composition.evidence.replay, **{field: value}))
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            changed,
        )
        == "REPLAY_JOURNAL_BINDING_MISMATCH"
    )


def test_replay_of_unsealed_persisted_journal(composition: Composition) -> None:
    raw = json.loads(composition.evidence.journal.canonical_serialization)
    raw["seal"] = None
    identity = composition.evidence.persistence.persistence_identity
    original = composition.journal_repository._values[identity]
    composition.journal_repository._values[identity] = canonical_json(raw)
    with pytest.raises(JournalFailure) as failure:
        replay_persisted(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            composition.journal_repository,
        )
    assert failure.value.code == "UNSEALED_JOURNAL"
    composition.journal_repository._values[identity] = original


def test_p_bound_to_wrong_j(composition: Composition) -> None:
    changed = _redigest_persistence(
        replace(composition.evidence.persistence, journal_root_digest="e" * 64)
    )
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            changed,
            composition.evidence.replay,
        )
        == "PERSISTENCE_JOURNAL_BINDING_MISMATCH"
    )


def test_r_bound_to_wrong_p(composition: Composition) -> None:
    changed = _redigest_replay(
        replace(composition.evidence.replay, persistence_identity="memory://journal/wrong")
    )
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            changed,
        )
        == "REPLAY_PERSISTENCE_BINDING_MISMATCH"
    )


def test_coherently_fabricated_persistence_identity(composition: Composition) -> None:
    identity = "journal://4b000000-0000-4000-8000-000000000027/" + "a" * 64
    persistence = _redigest_persistence(
        replace(composition.evidence.persistence, persistence_identity=identity)
    )
    replay = _redigest_replay(replace(composition.evidence.replay, persistence_identity=identity))
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            persistence,
            replay,
        )
        == "PERSISTENCE_OBJECT_IDENTITY_INVALID"
    )


def test_altered_p_receipt_digest(composition: Composition) -> None:
    changed = replace(composition.evidence.persistence, receipt_digest="0" * 64)
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            changed,
            composition.evidence.replay,
        )
        == "PERSISTENCE_RECEIPT_DIGEST_MISMATCH"
    )


def test_altered_r_receipt_digest(composition: Composition) -> None:
    changed = replace(composition.evidence.replay, receipt_digest="0" * 64)
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            changed,
        )
        == "REPLAY_RECEIPT_DIGEST_MISMATCH"
    )


def test_original_replay_projection_inequality(composition: Composition) -> None:
    changed = _redigest_replay(
        replace(composition.evidence.replay, replayed_projection_digest="1" * 64)
    )
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            changed,
        )
        == "REPLAY_PROJECTION_MISMATCH"
    )


def test_coherently_fabricated_projection_digests(composition: Composition) -> None:
    changed = _redigest_replay(
        replace(
            composition.evidence.replay,
            original_projection_digest="2" * 64,
            replayed_projection_digest="2" * 64,
        )
    )
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            changed,
        )
        == "REPLAY_PROJECTION_BINDING_MISMATCH"
    )


def test_record_after_finalization(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        clone = deepcopy(_record(raw, JOURNAL_KIND_EVENT_ACCEPTED, -1))
        clone["operation_id"] = "post-finalization-event"
        clone["timestamp"] = _record(raw, JOURNAL_KIND_SESSION_FINALIZED)["timestamp"]
        raw["records"].append(clone)

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "RECORD_AFTER_FINALIZATION"
    )


def test_broken_acknowledgement_correlation(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)["contract_identity"] = (
            "B:20260821:11000:C"
        )

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "ACK_CORRELATION_INVALID"
    )


def test_activation_before_required_acknowledgement(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        activation = _record(raw, JOURNAL_KIND_EPOCH_ACTIVATED)
        activation["parent_sequence"] = _record(raw, JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)[
            "sequence"
        ]

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "ACTIVATION_BEFORE_ACK"
    )


def test_accepted_event_for_removed_contract(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        final_event = _record(raw, JOURNAL_KIND_EVENT_ACCEPTED, -1)
        final_event["contract_identity"] = "A:20260821:10000:C"
        final_event["payload"]["symbol"] = "A"

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "ACCEPTED_EVENT_NOT_ACTIVE"
    )


def test_unsealed_journal_rejected(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization
    corrupted = _mutate_journal(serialized, lambda raw: raw.update(seal=None))
    assert _journal_failure(composition.contract, corrupted) == "UNSEALED_JOURNAL"


def test_changed_contract_run_binding(composition: Composition) -> None:
    changed = replace(
        composition.contract,
        expected_run_id=type(composition.contract.expected_run_id)(
            "4b000000-0000-4000-8000-000000000099"
        ),
    )
    assert (
        _projection_failure(
            changed,
            composition.evidence.journal,
            composition.evidence.persistence,
            composition.evidence.replay,
        )
        == "CONTRACT_RUN_BINDING_MISMATCH"
    )


def test_altered_payload_digest_without_record_digest(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        record = _record(raw, JOURNAL_KIND_CONFIGURATION)
        record["payload"]["ack_timeout"] = 999
        record["payload_digest"] = sha256(record["payload"])

    assert _journal_failure(composition.contract, _mutate_journal(serialized, mutate)) == (
        "RECORD_DIGEST_MISMATCH"
    )


def test_future_causal_parent(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)["parent_sequence"] = raw["records"][-1][
            "sequence"
        ]

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "CAUSAL_PARENT_MISSING"
    )


def test_fabricated_duplicate_lifecycle_record(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        stop_index = next(
            index for index, item in enumerate(raw["records"]) if item["kind"] == "intake.stopped"
        )
        clone = deepcopy(_record(raw, JOURNAL_KIND_CONFIGURATION))
        clone["operation_id"] = "fabricated-configuration"
        clone["timestamp"] = raw["records"][stop_index - 1]["timestamp"]
        raw["records"].insert(stop_index, clone)

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "DUPLICATE_RECORD"
    )


def test_single_record_from_another_run(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EVENT_ACCEPTED)["run_id"] = "4b000000-0000-4000-8000-000000000099"

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "RUN_ID_MISMATCH"
    )


def test_incorrect_restoration_epoch(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_RESTORED)["epoch_sequence"] = 1

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "RESTORATION_MEMBERSHIP_INVALID"
    )


def test_missing_channel_acknowledgement(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        target = next(
            item
            for item in raw["records"]
            if item["kind"] == JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT
            and item["epoch_sequence"] == 1
            and item["payload"]["channel"] == "QUOTE"
        )
        raw["records"].remove(target)

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "ACTIVATION_BEFORE_ACK"
    )


def test_incorrect_epoch_membership(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)["payload"]["membership"] = [
            "B:20260821:11000:C"
        ]

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "EPOCH_MEMBERSHIP_INVALID"
    )


def test_reevaluation_without_clock_cause(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        reevaluation = _record(raw, JOURNAL_KIND_REEVALUATION_START)
        reevaluation["parent_sequence"] = _record(raw, JOURNAL_KIND_EVENT_ACCEPTED)["sequence"]

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "CAUSAL_PARENT_WRONG"
    )


def test_clock_boundary_missing(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_CLOCK_ADVANCED)["payload"].pop("boundary")

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "CLOCK_BOUNDARY_MISSING"
    )


def test_altered_seal(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        raw["seal"]["record_count"] += 1

    assert _journal_failure(composition.contract, _mutate_journal(serialized, mutate)) == (
        "SEAL_RECORD_COUNT_MISMATCH"
    )


def test_journal_contract_version_mismatch(composition: Composition) -> None:
    serialized = composition.evidence.journal.canonical_serialization

    def mutate(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_CONFIGURATION)["payload"]["acceptance_contract_version"] = (
            "WRONG_CONTRACT"
        )

    assert (
        _journal_failure(composition.contract, _mutate_journal(serialized, mutate, rehash=True))
        == "JOURNAL_CONTRACT_VERSION_MISMATCH"
    )


def test_persistence_contract_version_mismatch(composition: Composition) -> None:
    changed = _redigest_persistence(
        replace(composition.evidence.persistence, contract_version="WRONG_CONTRACT")
    )
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            changed,
            composition.evidence.replay,
        )
        == "PERSISTENCE_CONTRACT_VERSION_MISMATCH"
    )


def test_replay_contract_version_mismatch(composition: Composition) -> None:
    changed = _redigest_replay(
        replace(composition.evidence.replay, contract_version="WRONG_CONTRACT")
    )
    assert (
        _projection_failure(
            composition.contract,
            composition.evidence.journal,
            composition.evidence.persistence,
            changed,
        )
        == "REPLAY_CONTRACT_VERSION_MISMATCH"
    )


def test_serialized_receipt_boolean_fabrication_rejected(composition: Composition) -> None:
    raw = composition.evidence.persistence.as_dict()
    raw["persistence_completed"] = "true"
    with pytest.raises(JournalFailure) as failure:
        PersistenceReceipt.deserialize(canonical_json(raw))
    assert failure.value.code == "PERSISTENCE_RECEIPT_DESERIALIZATION_FAILED"
