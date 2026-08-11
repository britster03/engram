# ADR 0001: Represent every extracted assertion as an immutable FACT

- Status: Accepted
- Date: 2026-08-11
- Owners: Memory/KG

## Context

Engram previously wrote FACT memories only for confidence values between 0.3
and 0.6. High-confidence entity assertions existed only as mutable derived
`RELATES_TO` edges, while high-confidence literal assertions could disappear
from the graph entirely. That mixed contract made provenance dependent on edge
properties and made runtime/rebuild equivalence difficult to prove.

## Decision

Every accepted, atomized triplet with confidence at least 0.3 is stored as one
immutable FACT file. Its URI is derived from tenant scope, ingest event ID,
triplet index, canonical relation, and object slug. The FACT envelope records
the source event, episode, session, source turn IDs, confidence, extractor,
temporal metadata, subject URI, and optional object URI.

- Entity-valued assertions retain a derived `RELATES_TO` edge for efficient
  graph traversal.
- Literal assertions remain retrievable FACT nodes and do not create phantom
  entity nodes.
- Low-confidence assertions remain `LOW_CONFIDENCE` and do not create a
  derived `RELATES_TO` edge.
- Duplicate source assertions are preserved and linked with `DUPLICATE_OF`;
  they do not create a duplicate derived edge.
- Contradictions retire the prior FACT projection and link the new FACT to it
  with `SUPERSEDES`. Entity identity nodes are never marked historical merely
  because an assertion changed.
- Runtime indexing and KG rebuild use the same FACT identity and reconstruct
  the same provenance and history edges.

## Compatibility

Existing schema-version-1 memories remain readable. Rebuild only places an
`assertion_uri` on a derived edge when the corresponding FACT file exists, so
pre-migration tenants do not acquire dangling graph references. A separate,
explicit migration may materialize FACT files for historical extraction rows;
normal crash recovery never rewrites completed events.

## Consequences

This increases filesystem and graph node counts roughly in proportion to the
number of extracted claims. In exchange, every assertion used by retrieval or
answer generation has stable, source-level provenance, literal claims survive
indexing, and contradictions no longer mutate entity identity state.
