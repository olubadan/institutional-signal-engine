"""Adversarial tests mutate the real persisted causal journal, never its certificate."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from copy import deepcopy
from typing import Any, cast

import pytest

from institutional_signal_engine.journal import (
    JOURNAL_KIND_CLOCK_ADVANCED,
    JOURNAL_KIND_CONFIGURATION,
    JOURNAL_KIND_EPOCH_ACTIVATED,
    JOURNAL_KIND_EPOCH_CREATED,
    JOURNAL_KIND_EPOCH_RESTORED,
    JOURNAL_KIND_EVENT_ACCEPTED,
    JOURNAL_KIND_JOURNAL_PERSISTED,
    JOURNAL_KIND_REPLAY_VERIFIED,
    JOURNAL_KIND_SESSION_FINALIZED,
    JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT,
    Journal,
    JournalFailure,
    canonical_json,
    sha256,
    verify_complete,
)
from institutional_signal_engine.phase4b_certify import (
    acceptance_projection,
    execute_composition,
    validate_certificate,
)

RawJournal = dict[str, Any]


@pytest.fixture(scope="module")
def canonical_serialized() -> str:
    return asyncio.run(execute_composition()).replayed.canonical_serialization


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


def _failure(serialized: str) -> str:
    try:
        verified = verify_complete(Journal.deserialize(serialized))
        acceptance_projection(verified)
    except JournalFailure as exc:
        return exc.code
    pytest.fail("corrupted journal unexpectedly passed production verification")


def _mutate(
    serialized: str, mutation: Callable[[RawJournal], None], *, rehash: bool = False
) -> str:
    raw = cast(RawJournal, json.loads(serialized))
    mutation(raw)
    if rehash:
        _rehash(raw)
    return canonical_json(raw)


def _delete_kind(raw: RawJournal, kind: str, occurrence: int = 0) -> None:
    target = _record(raw, kind, occurrence)
    raw["records"].remove(target)


def test_positive_composition_and_persisted_projection_are_exact(canonical_serialized: str) -> None:
    journal = Journal.deserialize(canonical_serialized)
    verified = verify_complete(journal)
    certificate = acceptance_projection(verified)
    replayed = verify_complete(Journal.deserialize(verified.canonical_serialization))
    assert acceptance_projection(replayed) == certificate
    assert certificate["overall"] == "PASS"
    assert validate_certificate(certificate) == []


def test_deleted_required_record(canonical_serialized: str) -> None:
    corrupted = _mutate(
        canonical_serialized,
        lambda raw: _delete_kind(raw, JOURNAL_KIND_CONFIGURATION),
        rehash=True,
    )
    assert _failure(corrupted) == "MISSING_REQUIRED_RECORD"


def test_reordered_records(canonical_serialized: str) -> None:
    def reorder(raw: RawJournal) -> None:
        raw["records"][5], raw["records"][6] = raw["records"][6], raw["records"][5]

    assert _failure(_mutate(canonical_serialized, reorder)) == "SEQUENCE_INVALID"


def test_altered_payload_without_updated_digest(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_CONFIGURATION)["payload"]["ack_timeout"] = 999

    assert _failure(_mutate(canonical_serialized, alter)) == "PAYLOAD_DIGEST_MISMATCH"


def test_altered_payload_digest_but_broken_record_chain(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        record = _record(raw, JOURNAL_KIND_CONFIGURATION)
        record["payload"]["ack_timeout"] = 999
        record["payload_digest"] = sha256(record["payload"])

    assert _failure(_mutate(canonical_serialized, alter)) == "RECORD_DIGEST_MISMATCH"


def test_invalid_causal_parent(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)["parent_sequence"] = raw["records"][-1][
            "sequence"
        ]

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "CAUSAL_PARENT_MISSING"


def test_plausible_but_wrong_earlier_parent(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        completion = next(r for r in raw["records"] if r["kind"] == "discovery.complete")
        completion["parent_sequence"] = _record(raw, JOURNAL_KIND_CONFIGURATION)["sequence"]

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "CAUSAL_PARENT_WRONG"


def test_missing_parent(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)["parent_sequence"] = None

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "CAUSAL_PARENT_WRONG"


def test_record_from_another_run(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EVENT_ACCEPTED)["run_id"] = "4b000000-0000-4000-8000-000000000099"

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "RUN_ID_MISMATCH"


def test_fabricated_extra_record(canonical_serialized: str) -> None:
    def fabricate(raw: RawJournal) -> None:
        raw["records"].insert(-2, deepcopy(_record(raw, JOURNAL_KIND_EVENT_ACCEPTED)))

    assert _failure(_mutate(canonical_serialized, fabricate)) == "SEQUENCE_INVALID"


def test_fabricated_record_with_recalculated_hashes(canonical_serialized: str) -> None:
    def fabricate(raw: RawJournal) -> None:
        stop_index = next(
            index
            for index, record in enumerate(raw["records"])
            if record["kind"] == "intake.stopped"
        )
        clone = deepcopy(_record(raw, JOURNAL_KIND_CONFIGURATION))
        clone["sequence"] = 9999
        clone["parent_sequence"] = raw["records"][stop_index - 1]["sequence"]
        clone["timestamp"] = raw["records"][stop_index - 1]["timestamp"]
        raw["records"].insert(stop_index, clone)

    assert _failure(_mutate(canonical_serialized, fabricate, rehash=True)) == "DUPLICATE_RECORD"


def test_broken_command_acknowledgement_correlation(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        ack = _record(raw, JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT)
        ack["contract_identity"] = "B:20260821:11000:C"

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "ACK_CORRELATION_INVALID"


@pytest.mark.parametrize("channel", ["TRADE", "QUOTE"])
def test_missing_channel_acknowledgement(canonical_serialized: str, channel: str) -> None:
    def alter(raw: RawJournal) -> None:
        target = next(
            record
            for record in raw["records"]
            if record["kind"] == JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT
            and record["epoch_sequence"] == 1
            and record["payload"]["channel"] == channel
        )
        raw["records"].remove(target)

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "ACTIVATION_BEFORE_ACK"


def test_activation_before_complete_acknowledgement(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        activation = _record(raw, JOURNAL_KIND_EPOCH_ACTIVATED)
        raw["records"].remove(activation)
        first_ack = next(
            record
            for record in raw["records"]
            if record["kind"] == JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT
            and record["epoch_sequence"] == 1
        )
        activation["parent_sequence"] = first_ack["sequence"]
        last_ack_index = max(
            index
            for index, record in enumerate(raw["records"])
            if record["kind"] == JOURNAL_KIND_SUBSCRIPTION_ACKNOWLEDGEMENT
            and record["epoch_sequence"] == 1
        )
        raw["records"].insert(last_ack_index, activation)

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "ACTIVATION_BEFORE_ACK"


def test_incorrect_epoch_membership(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_EPOCH_CREATED, 1)["payload"]["membership"] = [
            "B:20260821:11000:C"
        ]

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == (
        "SUBSCRIPTION_DIFF_INCOMPLETE"
    )


def test_accepted_event_for_removed_contract(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        target = next(
            record
            for record in raw["records"]
            if record["kind"] == JOURNAL_KIND_EVENT_ACCEPTED and record["epoch_sequence"] == 3
        )
        target["contract_identity"] = "A:20260821:10000:C"

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == (
        "ACCEPTED_EVENT_NOT_ACTIVE"
    )


def test_incorrect_restoration_epoch(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        restored = _record(raw, JOURNAL_KIND_EPOCH_RESTORED)
        restored["epoch_sequence"] = 1

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == (
        "RESTORATION_EPOCH_INVALID"
    )


def test_reevaluation_without_clock_cause(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        reevaluation = next(r for r in raw["records"] if r["kind"] == "reevaluation.start")
        reevaluation["parent_sequence"] = _record(raw, JOURNAL_KIND_EVENT_ACCEPTED)["sequence"]

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "CAUSAL_PARENT_WRONG"


def test_record_after_finalization(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        final_index = next(
            index
            for index, record in enumerate(raw["records"])
            if record["kind"] == JOURNAL_KIND_SESSION_FINALIZED
        )
        clone = deepcopy(_record(raw, JOURNAL_KIND_EVENT_ACCEPTED))
        clone["sequence"] = 9999
        clone["timestamp"] = raw["records"][final_index]["timestamp"]
        raw["records"].insert(final_index + 1, clone)

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == (
        "RECORD_AFTER_FINALIZATION"
    )


def test_altered_seal(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        raw["seal"]["record_count"] += 1

    assert _failure(_mutate(canonical_serialized, alter)) == "SEAL_RECORD_COUNT_MISMATCH"


def test_unsealed_journal_and_projection_rejected(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        raw["seal"] = None

    corrupted = _mutate(canonical_serialized, alter)
    assert _failure(corrupted) == "UNSEALED_JOURNAL"
    with pytest.raises(JournalFailure) as failure:
        acceptance_projection(cast(Any, Journal.deserialize(corrupted)))
    assert failure.value.code == "PROJECTION_REQUIRES_VERIFIED_JOURNAL"


def test_persisted_replay_mismatch(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        replay = _record(raw, JOURNAL_KIND_REPLAY_VERIFIED)
        replay["payload"]["exact_equal"] = False

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "REPLAY_MISMATCH"


def test_clock_payload_corruption_is_detected(canonical_serialized: str) -> None:
    def alter(raw: RawJournal) -> None:
        _record(raw, JOURNAL_KIND_CLOCK_ADVANCED)["payload"].pop("boundary")

    assert _failure(_mutate(canonical_serialized, alter, rehash=True)) == "CLOCK_BOUNDARY_MISSING"


def test_journal_persistence_record_is_not_synthetic(canonical_serialized: str) -> None:
    verified = verify_complete(Journal.deserialize(canonical_serialized))
    persisted = verified.records_by_kind(JOURNAL_KIND_JOURNAL_PERSISTED)[0]
    assert persisted.payload["persisted_identity"].startswith("memory://journal/")
    assert persisted.payload["queue_depth_final"] == 0
