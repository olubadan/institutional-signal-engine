# Monday live-candidate integration requirement

This offline mission does not validate a provider or Oracle. Before any live
validation begins, the exact Monday candidate must prove all of the following
on the requested target:

1. The repository identity and resulting commit are verified; no substitute
   checkout or historical evidence is accepted.
2. Newly normalized option and equity events retain provider/event time,
   provider sequence, local receive wall time, local monotonic receive time,
   normalization time, and provenance.
3. Pipeline admission records `_pipeline_admission_timestamp`, and the durable
   `canonical_event_receipts` journal records local wall time after canonical
   insert/flush. These remain separate clocks and receipt stages.
4. The option transition receipt records `qualification_timestamp` when the
   existing qualification rule first becomes true, even if the cluster is
   still growing.
5. The emitted qualifying transition records `emission_timestamp`.
6. Historical receipts missing these fields remain explicitly incomplete;
   they are not backfilled from later timestamps.
7. Existing CONTROL and SHADOW definitions, thresholds, and observation-only
   safety invariants remain unchanged.
8. The provider/session identity, one authoritative event consumer, and live
   evidence destination are independently verified before provider contact.

Only after these checks pass may a separate live-market mission begin. The
synthetic laboratory, its ground truth, and its results are not live evidence
and cannot satisfy this requirement.
