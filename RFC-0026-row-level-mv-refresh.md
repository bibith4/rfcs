# RFC-0026 for Presto: Row-Level Incremental Materialized View Refresh

## Proposers

* Shijin(bibith4)
* Nandu (Nandakumar Balagopal)
* Dilli (Dilli Babu Godari)
## Related Issues

* RFC-0016 — Materialized Views (foundational architecture)
* prestodb/presto#26959: partition-level incremental refresh (IncrementalRefreshRule, DifferentialPlanRewriter)
* prestodb/presto#27774: bounded refresh (max_snapshots_per_refresh)
* prestodb/presto#27816: fallback warnings for stitching and refresh
* prestodb/presto#27820: cost-based selection of MV rewrite candidates

## Summary

This RFC proposes row-level incremental refresh for Presto materialized views — the third refresh
strategy alongside full recompute and partition-level incremental refresh.

Partition-level refresh, caps cost at the partition grain: a single changed row
in a 500M-row partition triggers a full partition recompute. Row-level refresh matches the actual
change rate. When few rows change inside large partitions, row-level wins decisively over both
full recompute and partition-level; when most rows change the cost model rejects it in favour of
full recompute. The decision is always cost-based — the existing
`MVRewriteCandidatesNode` + `SelectLowestCostMVRewrite` framework (PR #27820) picks the cheapest
candidate from the set.

The design spans three surfaces:

1. **SPI extension** — four new `ConnectorMetadata` methods, two new `MaterializedViewStatus`
   fields, and two new SPI types that give connectors a uniform way to expose row-level change
   tracking. All methods carry default no-op implementations; existing connectors require no
   change.
2. **Engine extension** — `DifferentialPlanRewriter` is extended (not replaced) to build a
   third candidate whose leaves use row-level predicates for V3-capable bases and partition-level
   predicates for V2 bases, mixing granularity within the same plan.
3. **Iceberg connector implementation** — `IcebergAbstractMetadata` implements the new SPI
   against Iceberg V3 row lineage (`_row_id`, `_last_updated_sequence_number`,
   `IncrementalChangelogScan`).

## Background

### The Problem

The dominant production materialized-view workload is CDC-fed BI: a wide fact table receives a
small fraction of row updates per refresh window. Partition-level refresh's effective invalidation
rate is determined by the partition layout, not the actual change rate. When one row changes in a
100M-row monthly partition, partition-level recomputes the entire partition.

| Strategy | Cost when 1 row in 10M changes | Cost when 90% of rows change |
|---|---|---|
| Full recompute | High | High |
| Partition-level  | Medium (whole partition) | High (≈ full) |
| Row-level (this design) | Low (proportional to change set) | High (cost picker rejects in favour of full) |


Without row-level, the existing bounded-refresh ceiling (`max_snapshots_per_refresh`) is the only knob
to limit cost — a freshness-vs-cost tradeoff that is not workload-aware. Row-level is the
workload-aware version of the same goal.

### Required Primitive — Iceberg V3 Row Lineage

Row-level invalidation requires durable row identity. Iceberg V3 provides it:

| Field | Definition | Use here |
|---|---|---|
| `_row_id` | Long, assigned at first write; preserved by copy-on-write and compaction | Row's stable identity |
| `_last_updated_sequence_number` | Sequence number of the snapshot that last modified the row | "Did this row change since snapshot X?" predicate carrier |

Crucially, during copy-on-write rewrites `_row_id` is preserved. A compaction that rewrites a
file does not invalidate the identity of rows whose content did not change. This is exactly the
property row-level invalidation needs.

V2 tables lack durable row identity (positional deletes reference file paths that compaction
destroys; equality deletes do not uniquely identify rows). V2 base tables fall back to
partition-level via the cost picker; no engine code changes when a connector returns
`Optional.empty()` from `getCurrentTableVersion`.

### Why V3 but Not "Any Iceberg with Delete Files"

The anti-join operation that identifies deleted rows is only well-defined when row identity
survives across rewrites. Under V2, positional deletes reference `(file_path, position)` pairs
that become meaningless after compaction, and equality deletes identify rows by predicate, not
uniquely. V3 deletion vectors, combined with V3 `_row_id`, resolve to a set of stable row
identities — exactly what the anti-join needs.

### Goals

* Deliver row-level incremental refresh for aggregating MVs (all aggregation functions via
  Case B expansion; SUM/COUNT additionally via classical IVM).
* Deliver row-level incremental stitching at query time as a third candidate the cost-based
  picker considers alongside partition-level stitching and full recompute.
* Keep existing connectors completely unaffected via default no-op SPI implementations.
* Iceberg V3 base tables use row-level leaves; Iceberg V2 bases fall back to partition-level
  automatically within the same MV candidate (mixed granularity).
* Provide session and per-MV controls for enabling, disabling, or forcing the strategy.

### Non-Goals

* Row-preserving MVs with joins between base tables (v2 task `mv-row-preserving-multi-base`).
* Classical IVM for AVG/STDDEV/MIN/MAX (v2 tasks `mv-row-level-classical-ivm-avg-stddev`,
  `mv-row-level-classical-ivm-min-max`).
* Distributed execution of the change-set scan — v1 runs COORDINATOR_ONLY (v2 task
  `mv-row-level-distribute-changes-tvf`).
* `affected_identifiers` CTE sharing (v2 task `mv-row-level-cte-share`).
* Non-Iceberg connectors (Delta, JDBC) — independent v2 tasks.
* Benchmarking and threshold tuning.
* Backfilling row lineage on existing Iceberg V2 tables.

## Proposed Implementation

### 1. SPI Extension

Four new methods on `ConnectorMetadata`, two new fields on `MaterializedViewStatus`, and three
new types. All methods carry default no-op / empty implementations; existing connectors are
unaffected.

#### 1.1 New `ConnectorMetadata` Methods

```java
// Version embedded in a table handle. Non-empty return = this table supports row-level
// change tracking. Opaque to the engine; round-tripped through MV metadata.
default Optional<ConnectorTableVersion> getCurrentTableVersion(
        ConnectorSession session,
        ConnectorTableHandle table)
{
    return Optional.empty();
}

// Page source streaming changes between two versions. Output schema:
//   [ChangeKind: ChangeKindEnumType, $rowId, ...projectedDataColumns]
// DELETE / UPDATE_BEFORE records MUST surface pre-state values for projected columns
// (aggregating MVs need them to compute affected groups from deleted rows).
default ChangeKindPageSource getChangeSet(
        ConnectorSession session,
        ConnectorTableHandle table,
        ConnectorTableVersion from,
        ConnectorTableVersion to,
        List<ColumnHandle> projectedDataColumns,
        TupleDomain<ColumnHandle> filter)
{
    throw new UnsupportedOperationException("row-level change tracking not supported");
}

// Expected row-count cardinality of the change set (not bytes). Read from snapshot
// metadata without data-file access where possible. Consumed by the cost picker.
default OptionalLong estimateChangeSetSize(
        ConnectorSession session,
        ConnectorTableHandle table,
        ConnectorTableVersion from,
        ConnectorTableVersion to)
{
    return OptionalLong.empty();
}

// Whether the MV storage table can atomically replace affected rows/groups.
// Connectors must opt in before the engine enables row-level MV refresh planning.
default boolean supportsMaterializedViewRowLevelRefresh(
        ConnectorSession session,
        ConnectorTableHandle materializedViewTable)
{
    return false;
}
```

**Why these methods belong on `ConnectorMetadata`**: Presto's convention for per-table
capabilities is `ConnectorMetadata` methods (MERGE family, stats methods, time-travel). Separate
provider interfaces are reserved for top-level per-connector concerns. Row-level change tracking
is per-table, so it follows the `ConnectorMetadata` pattern.

#### 1.2 New Fields on `MaterializedViewStatus`

```java
public class MaterializedViewStatus
{
    // ... existing fields unchanged ...

    // NEW: table handles pinned at the recorded base version. The engine calls
    // getCurrentTableVersion(recordedHandle) to extract the recorded version,
    // then passes it as `from` to getChangeSet.
    private final Map<SchemaTableName, ConnectorTableHandle> recordedBaseTableHandles;

    // NEW: per-base stale-row predicate, replacing (and deprecating)
    // partitionsFromBaseTables. Bundles:
    //   dataDisjuncts:  OR'd stale-row predicates over (recorded, HEAD]
    //   refreshBound:   refresh-only sequence-number cap (TupleDomain.all() = no cap)
    private final Map<SchemaTableName, ChangedRowsPredicate> changedRowsPredicates;
}
```

`partitionsFromBaseTables` is deprecated (annotated `@Deprecated`) but not removed — it remains
the only mechanism for a base table without row lineage and the input to partition-level stitching.

**Why these fields live on `MaterializedViewStatus`**: Today's `partitionsFromBaseTables` is
exactly the carrier shape — a per-MV, per-base predicate map populated by a single
`getMaterializedViewStatus` call. Adding a separate `getChangedRowsPredicate(table, since)` method
would duplicate the surface without earning anything (the engine always passes
`since = recorded`, sourced from the same status object). The upgrade is in-place via two new
fields and one method call still populates everything.

#### 1.3 New SPI Types

**`ChangedRowsPredicate`** — immutable value type bundling the per-base stale-row predicate:

```java
public class ChangedRowsPredicate
{
    // Disjuncts over (recorded, HEAD] — stale-row predicate consumed by both the
    // stale-read path and the refresh delta branch.
    private final List<TupleDomain<ColumnHandle>> dataDisjuncts;

    // Refresh-only sequence-number cap. Applied by the refresh rule; NOT applied on
    // stale reads (which must reflect HEAD, not be capped at a target snapshot).
    private final TupleDomain<ColumnHandle> refreshBound;
}
```

`dataDisjuncts` unifies today's partition-level disjuncts and the row-level
`_last_updated_sequence_number > recorded_seq` case; the engine treats them uniformly.
`refreshBound` integrates with PR #27774's bounded-refresh machinery.

**`ChangeKindPageSource extends ConnectorPageSource`** — marker interface signalling that one
column of the produced pages has type `ChangeKindEnumType`. The engine constructs the output
schema at plan time (the schema is engine-owned for this engine-registered TVF), so the column's
identity is known before any page source exists. The marker only signals semantic content; it
carries no runtime accessors, matching the marker-interface precedent in `presto-spi`
(`ConnectorOutputTableHandle`, etc.).

**`ChangeKindEnumType extends VarcharEnumType`** — a `VarcharEnumType` with four values
(`INSERT`, `DELETE`, `UPDATE_BEFORE`, `UPDATE_AFTER`), registered at engine startup via new
built-in UDT registration infrastructure (~40 LOC in
`BuiltInTypeAndFunctionNamespaceManager`). The type-system enforces the value space: any column
of type `ChangeKindEnumType` is guaranteed to contain only those four strings. SQL filter
expressions over the column type-check against the enum's allowed values; `EXPLAIN` output shows
the named type; users calling `system.changes(...)` from raw SQL can write
`WHERE kind = change_kind.DELETE`.

| `ChangeKind` value | Meaning | Engine consumption |
|---|---|---|
| `INSERT` | Row added in the version range; currently present | Drives `affected_groups` / `affected_identifiers` |
| `DELETE` | Row removed; not present at `to` | Surfaces deleted-row identities for fresh-branch anti-join |
| `UPDATE_BEFORE` | Pre-image of an in-place UPDATE | Treated as DELETE for identity purposes |
| `UPDATE_AFTER` | Post-image of an in-place UPDATE | Treated as INSERT for affected-row purposes |

**Pairing contract**: A connector that implements `getChangeSet` (non-throwing) MUST also:
return non-empty from `getCurrentTableVersion` for the same table; populate
`MaterializedViewStatus.recordedBaseTableHandles` for every MV base table; and populate
`MaterializedViewStatus.changedRowsPredicates` for those same bases. All four pieces signal "this
table supports versioned change tracking." The engine declines row-level refresh rather than
risking a wrong answer when predicates are populated without matching pinned handles.

#### 1.4 `ConnectorTableVersion` Enhancements

`ConnectorTableVersion` is enhanced to be safely serializable (JSON-annotated with block-backed
serialization for the version value), type-validated at construction, and `equals`/`hashCode`
comparable. The version value is a type-native Java object round-tripped as a single-position
block the way `ConstantExpression` does, so a BIGINT snapshot ID cannot come back as `Integer`
after deserialization.

### 2. `system.changes` Table-Valued Function

The engine registers `system.changes(table_ref, from_version, to_version)` as a TVF in the
system catalog. Its execution:

1. Resolves `table_ref` to (catalog, table handle, column handles).
2. Calls `metadata.getCurrentTableVersion(table)` to verify row-level capability.
3. If empty → error ("row-level tracking not supported on this table").
4. If present → builds a `TableScan`-equivalent backed by `metadata.getChangeSet(...)`, whose
   output schema is `[ChangeKind: ChangeKindEnumType, $rowId, ...projected data columns]`.

The TVF is user-callable from SQL:
```sql
SELECT * FROM system.changes(iceberg.db.sales, FROM 'v1', TO 'v2')
```

**COORDINATOR_ONLY for v1**: `system.changes` invocations execute on the coordinator in v1. The
coordinator scans the change-set locally; the result is broadcast to workers as a small relation
(broadcast-join pattern). This works because the cost picker only selects the row-level candidate
when the change set is small; large change sets are rejected in favour of full recompute. No C++
worker implementation is required for v1.

### 3. Plan Shapes

#### 3.1 `affected_identifiers` — the Central Helper

A single helper construction drives both the fresh branch's anti-join and the delta branch's scan:

```
affected_identifiers(base, identifier_columns, changedRowsPredicates)
  = DISTINCT(
        from_current_base         -- changed rows still present at pinned snapshot
    )

from_current_base
  = TableScan(base)
      .filter(OR(changedRowsPredicates[base].dataDisjuncts))
      .project(identifier_columns)
      .distinct(identifier_columns)
```

`identifier_columns` is determined by MV shape:
- **Aggregating MV**: the `GROUP BY` columns.
- **Row-preserving MV (single-base, v1)**: deferred — needs `$rowId_origin` storage column.

The disjuncts are the connector's row-level `_last_updated_sequence_number > recorded_seq`
predicates (row-level candidate) or the partition-level stale predicates (partition-level
candidate). The construction is identical in both cases, which is the point of the shared helper.

#### 3.2 Fresh Branch — Anti-Join Shape

```
fresh_branch
  = ANTI JOIN(MV_storage, affected_identifiers)
    ON NOT(storage.groupCol IS DISTINCT FROM affected.groupCol) [per identifier column]
```

The `NOT IS DISTINCT FROM` join criterion handles null group keys correctly (a null key is a
group of its own) and is the standard pattern for null-safe equi-joins in Presto.

For partition-level bases, the fresh branch continues to use `Filter(NOT stale_predicate)`
directly over the storage table; the anti-join form is reserved for row-level bases because:
- A partition-level predicate maps to storage table columns via pass-through equivalences;
  `NOT(predicate)` is expressible over those columns.
- A row-level `changedRowsPredicates` is expressed over change-tracking columns
  (`_last_updated_sequence_number`) that have no equivalent on the storage table.

#### 3.3 Delta Branch — Aggregating MV

The delta plan for an aggregating MV uses **Case B expansion** (semi-join expansion), which is
correct for all aggregation functions including holistic ones (MEDIAN, COUNT DISTINCT):

```
delta_branch
  = γ_AGG GROUP BY identifier_columns
    [
      SEMI JOIN(TableScan(base @pinned), affected_identifiers)
      ON base.groupCol = affected.groupCol
    ]
    .project(result_columns)
```

The semi-join fetches all rows of every affected group from the current base, then the aggregation
recomputes the group from scratch. No function-specific decomposition is required; correctness
follows from reading the full current state of every affected group.

**Case A** (direct `γ(∆R)`) is available when the delta closure — the set of columns the stale
predicate is expressed over — is a subset of the `GROUP BY` columns. Row-level predicates are
expressed over `_last_updated_sequence_number`, not `GROUP BY` columns, so row-level always uses
Case B.

**Classical IVM for SUM/COUNT**: a change-set scan → signed projection → `γ` → `MERGE` path
avoids the base-table scan entirely. Cost is proportional to the change set, not to affected-group
sizes:

```
MERGE INTO MV_storage ON MV_storage.region = delta.region
  WHEN MATCHED:     total = total + delta.delta_sum
  WHEN NOT MATCHED: INSERT (region, delta.delta_sum)
      |
  γ_SUM(signed_amount) GROUP BY region
      |
  project(region,
    CASE ChangeKind
      WHEN INSERT, UPDATE_AFTER  THEN  amount
      WHEN UPDATE_BEFORE, DELETE THEN -amount
    END AS signed_amount)
      |
  TableScan(system.changes(sales, recorded, pinned))
```

This is what Oracle FAST refresh, SQL Server Indexed Views, and Snowflake Dynamic Tables do for
the dominant aggregating MV workload class. v1 ships it because SUM/COUNT aggregating MVs over
CDC fact tables are the primary target.

#### 3.4 Example: Aggregating MV, Row-Level (Case B)

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT region, SUM(amount) AS total FROM sales GROUP BY region
```

Stitched plan at query time:

```
UNION ALL
/                              \
fresh_branch                   delta_branch
     |                              |
ANTI JOIN on region           γ_SUM(amount) GROUP BY region
/        \                         |
MV_storage   affected_        SEMI JOIN on region
             identifiers      /              \
             (region)   TableScan(sales)  affected_identifiers
                        @pinned            (region)
```

#### 3.5 Example: Row-Preserving MV

```sql
CREATE MATERIALIZED VIEW mv AS
SELECT customer_id, order_id, amount FROM sales WHERE region = 'NA'
```

Deferred to v2 (`mv-row-preserving-multi-base`): the storage table needs a `$rowId_origin`
column populated at every refresh for the anti-join to key on. Without it the fresh branch has
no identity column to join against.

#### 3.6 The UNION ALL Structure

```
              UNION ALL
              /        \
       fresh_branch   delta_branch
```

The `UNION ALL` node is unchanged from the partition-level case. The only difference is how each
branch identifies "affected" rows: partition-level uses `Filter(NOT stale_predicate)` + predicate
matching on the delta side; row-level uses the anti-join and `affected_identifiers` on both sides.

### 4. Engine Changes — `DifferentialPlanRewriter` Extensions

The rewriter is extended, not replaced. The IVM composition algebra is preserved.

#### 4.1 Row-Level Candidate Construction (`MaterializedViewRewrite.apply`)

1. Read `MaterializedViewStatus` (existing SPI, extended with two new fields).
2. Check `status.hasRowLevelChanges()` — if true, attempt to build a row-level candidate.
3. Validate the SPI pairing contract: `changedRowsPredicates.keySet()` must be a subset of
   `recordedBaseTableHandles.keySet()`. If not, warn and skip.
4. Construct `DifferentialPlanRewriter` with `changedRowsPredicates` — bases present in the map
   get row-level delta leaves; bases absent keep partition-level leaves.
5. Build `fresh_branch` via `buildDataTableBranch` — for row-level bases, emit
   `ANTI JOIN(MV_storage, affected_identifiers)` instead of `Filter(NOT stale_predicate)`.
6. Build `delta_branch` via `buildDeltaPlan` — Case B expansion for aggregating MVs.
7. Build `UNION ALL` and add as a third candidate in `MVRewriteCandidatesNode`.

`buildRowLevelPlan` uses a `BufferingWarningCollector` to capture row-level-specific decline
reasons — surfaced with context ("falling back to partition-level stitching" vs "falling back to
full recompute") rather than discarded.

#### 4.2 `buildDataTableBranch` — Per-Base Mechanism Selection

```java
for (entry : constraints) {
    baseTable = entry.getKey();
    if (entry.getValue().isEmpty() || isRowLevel(changedRowsPredicates.get(baseTable))) {
        continue;   // row-level base: handled by anti-join below
    }
    // partition-level: Filter(NOT stale_predicate) over storage table
    storagePredicates.addAll(equivalentDataTablePredicates(...));
}
// row-level bases: anti-join against affected_identifiers
for (entry : changedRowsPredicates) {
    if (!isRowLevel(entry.getValue())) { continue; }
    freshPlan = antiJoinAffectedIdentifiers(...);
}
```

Exactly one mechanism applies per base — applying both would exclude more from the fresh branch
than the delta branch recomputes, dropping rows.

#### 4.3 `buildAffectedIdentifiers`

The affected-identifiers relation is built as:

```
TableScan(base @pinned)
  .filter(OR(changedRowsDisjuncts))       // changed-rows predicate or partition predicate
  .distinct(identifier_columns)           // AggregationNode with empty aggregate map
  .project(identifiers + TRUE_CONSTANT marker)
```

The marker column (`TRUE_CONSTANT`) makes the anti-join's unmatched side detectable as
`IS NULL(marker)` on the LEFT JOIN output.

#### 4.4 `resolveGroupingIdentifiers` — v1 Scope Guard

Declines (throws `UnsupportedOperationException`) for:
- Views with `JoinNode` — v2 task `mv-row-preserving-multi-base`.
- Views with no aggregation — row-preserving, needs `$rowId_origin`.

The `UnsupportedOperationException` is caught by the stitching fallback boundary in
`buildStitchedPlan`, which downgrades to partition-level if available, or full recompute.

#### 4.5 `resolveBaseColumnsByVariable` — Rename Propagation

`GroupIdNode` renames `region` to `region$gid`; a standard `ProjectNode` may rename further.
`resolveBaseColumnsByVariable` follows rename chains (pure variable-to-variable assignments) until
convergence, so a grouping key that has been renamed still resolves to its base table column. Only
renames are followed; computed expressions are deliberately left unresolved.

#### 4.6 `rowLevelReplacementIsSafe` — Refresh Safety Gate

Row-level refresh at refresh time (`IncrementalRefreshRule`) must verify that writing only the
affected groups is safe to commit. Writing a partial delta is safe only when the storage table is
partitioned by exactly the view's grouping columns: then a partition is a group, and the refresh
replaces those partitions atomically. Coarser partitioning would rewrite a partition from a subset
of its groups and discard the rest.

```java
public static boolean rowLevelReplacementIsSafe(
        Metadata metadata,
        Session session,
        PlanNode viewQueryPlan,
        SchemaTableName dataTable,
        PassthroughColumnEquivalences columnEquivalences,
        Set<String> storagePartitionColumns,
        Lookup lookup)
```

Returns `false` (falls back to partition-level refresh) if:
- Storage table has no partition columns.
- View query contains a join.
- View query has ≠ 1 aggregation node.
- Any grouping key does not trace back to a base table column.
- The set of storage columns corresponding to the grouping keys ≠ the set of storage partition
  columns.

#### 4.7 `IncrementalRefreshRule` — Row-Level Refresh Path

```java
// Row-level refresh path (in IncrementalRefreshRule.apply)
if (status.hasRowLevelChanges()
        && status.hasPartitionRefreshData()           // connector-side commit safety
        && RowLevelRefreshEnablement.isEnabled(session, definition.getRowLevelIncrementalRefresh())
        && rowLevelReplacementIsSafe(...)) {
    rowLevelPredicates = status.getChangedRowsPredicates();
} else {
    // warn and stay partition-level
}
```

The `hasPartitionRefreshData()` check is required even for row-level: the connector uses the
partition metadata to decide how to commit (Iceberg replaces affected partitions). Without it,
Iceberg's `finishRefreshMaterializedView` would overwrite the entire storage table.

`refreshBound` (from `ChangedRowsPredicate`) is applied as an additional filter on every base
table scan in the refresh plan via `applyIncrementalRefreshPredicates`. This integrates with PR
#27774's bounded-refresh machinery: the bound caps how far a refresh advances, so a refresh that
fails mid-way resumes from the right watermark. The bound is NOT applied to stale-read plans
(those must reflect HEAD).

### 5. Iceberg V3 Connector Implementation

`IcebergAbstractMetadata` implements the four new `ConnectorMetadata` methods:

| Method | V3 table | V2 table |
|---|---|---|
| `getCurrentTableVersion` | Return `ConnectorTableVersion.SNAPSHOT_ID(snapshotId)` from handle | Return `Optional.empty()` |
| `getChangeSet` | Back by `IncrementalChangelogScan`; walk snapshot range; emit `ChangeKindPageSource` | (unreachable — `getCurrentTableVersion` returned empty) |
| `estimateChangeSetSize` | Read `added-rows` + deletion-vector cardinality from snapshot metadata (no data-file access) | Return `OptionalLong.empty()` |
| `getMaterializedViewStatus` | Populate `recordedBaseTableHandles` + `changedRowsPredicates` | Existing partition-level path unchanged |

**`getMaterializedViewStatus` extension**:
- `recordedBaseTableHandles`: handles pinned at the recorded snapshot, constructed as
  `getTableHandle(name, FOR VERSION AS OF <recordedVersion>)`. One read populates the existing
  `partitionsFromBaseTables` plus both new fields.
- `changedRowsPredicates`:
  - `dataDisjuncts` = `_last_updated_sequence_number > recorded_seq` as a
    `TupleDomain<ColumnHandle>` keyed on Iceberg's sequence-number column.
  - `refreshBound` = `_last_updated_sequence_number <= target_seq` (from PR #27774's
    bounded-refresh machinery).

**`getChangeSet` for V3**:
1. Create an `IncrementalChangelogScan` over the snapshot range `[from, to]`.
2. For each `ChangelogScanTask`, emit rows with `ChangeKind` populated from the task's
   operation type.
3. `DELETE` and `UPDATE_BEFORE` rows MUST surface pre-state column values (needed by aggregating
   MVs to determine which groups are affected by deletions).

**`supportsMaterializedViewRowLevelRefresh`**: return `true` for V3 tables that support atomic
partition replacement (Iceberg MoR write mode or equivalent).

### 6. Session Properties and Controls

Two independent controls, each decisive in one direction only:

| Control | Values | Default | Semantics |
|---|---|---|---|
| Session property `materialized_view_row_level_incremental_strategy` | `ALWAYS` / `NEVER` / `AUTOMATIC` | `AUTOMATIC` | `NEVER` suppresses row-level regardless of MV property |
| MV property `row_level_incremental_refresh` | `true` / `false` / unset | unset (auto) | `false` opts out regardless of session |

Session strategy wins over MV property for suppression; MV `false` wins over session enablement.
A view that sets nothing defers to the session strategy. Under `AUTOMATIC` the cost picker
decides; under `ALWAYS` row-level is attempted whenever eligible; under `NEVER` it is suppressed.

### 7. Cost Picker Integration

The row-level candidate is added to `MVRewriteCandidatesNode` alongside the partition-level and
full-recompute candidates. `SelectLowestCostMVRewrite` compares them via the existing
`CostProvider` machinery:

- **Row-level leaf cardinality**: `estimateChangeSetSize(recorded, pinned)`.
- **Whole-plan cost**: standard `StatsCalculator` propagation through the Case B expansion.

**Known V1 cost-picker biases** (both conservative — overpay rather than under-deliver):

1. `affected_identifiers` is built as two independent subtrees per consumer (once for the
   anti-join, once for the delta semi-join) rather than a shared CTE. V2 fix:
   `CteProducerNode`/`CteConsumerNode` sharing (`mv-row-level-cte-share`).
2. `estimateChangeSetSize` uses snapshot-metadata upper bounds that overstate actual cardinality
   after deletion-vector application. V2 fix: improved connector-side estimation.

### 8. Warning Codes

Two new `StandardWarningCode` values:

| Code | When emitted |
|---|---|
| `MATERIALIZED_VIEW_STITCHING_FALLBACK` (existing) | Row-level was considered but declined (SPI contract violation, unsupported MV shape, storage partition mismatch, etc.) |
| `MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST` (new) | Row-level was eligible and built but the cost picker chose a cheaper candidate. Distinct from a fallback — the mechanism worked as intended |

### 9. Correctness Properties

The stitched plan satisfies four invariants:

1. **No duplicates**: `fresh_branch` and `delta_branch` are disjoint by construction —
   `affected_identifiers` is the partition that separates them.
2. **No missing rows**: `affected_identifiers` is exhaustive over all changes between `recorded`
   and `pinned`; no committed change is omitted.
3. **Monotonic freshness**: the freshness verdict (pinned snapshot IDs) is computed once at plan
   time; base-table commits during query execution do not change the plan.
4. **Snapshot equivalence**: the stitched result is equivalent to a full recompute at each
   base's pinned snapshot.

**Aggregation correctness**: Case B reads every row of every affected group from the current base.
Correct for any aggregation function including holistic (MEDIAN, COUNT DISTINCT) because no
function-specific decomposition is required.

### 10. V1 Scope and Restrictions

**In scope for v1**:
- Aggregating MVs (all aggregation functions via Case B; SUM/COUNT additionally via classical IVM)
- Iceberg V3 base tables; V2 bases fall back to partition-level automatically
- Mixed granularity: V3 bases use row-level leaves, V2 bases use partition-level leaves, within
  the same MV candidate
- `system.changes` TVF — COORDINATOR_ONLY execution; also user-callable from SQL
- `ChangeKindEnumType` registered as a built-in engine UDT at startup
- Cost-based selection among row-level, partition-level, and full-recompute candidates

**V1 restrictions (explicitly deferred)**:
- Row-preserving MVs (single-base, no joins) → v2 task `mv-row-preserving-multi-base`
- Classical IVM for AVG/STDDEV/MIN/MAX → Case B expansion in v1
- `affected_identifiers` CTE sharing → independent subtrees in v1
- Distributed `system.changes` execution → COORDINATOR_ONLY in v1
- Non-Iceberg connectors (Delta, JDBC) → v2 independent tasks

### 11. Implementation Task Breakdown

Ordered by dependency. Tasks within a tier can parallelize.

**Tier 0 — Foundations (unblock everything else)**

| Task | Description | Status |
|---|---|---|
| `mv-row-level-spi-metadata-methods` | All new SPI: `getCurrentTableVersion`, `getChangeSet`, `estimateChangeSetSize`; two new `MaterializedViewStatus` fields; `ChangedRowsPredicate`; `ChangeKindPageSource`; `ChangeKindEnumType` + built-in UDT registration; `ConnectorTableVersion` serialization enhancements | [OSS PR #28451](https://github.com/prestodb/presto/pull/28451) — open |

**Tier 1 — Core Engine**

| Task | Description |
|---|---|
| `mv-row-level-system-changes-tvf` | Register `system.changes(table_ref, from, to)` as a TVF in the system catalog; COORDINATOR_ONLY for v1; output schema engine-owned |
| `mv-partition-level-affected-identifiers-refactor` | Refactor partition-level stitching to use the unified `affected_identifiers` helper (anti-join / semi-join shape). Self-contained against existing tests |

**Tier 2 — Rewriter Extensions (requires Tier 1)**

| Task | Description |
|---|---|
| `mv-row-level-engine-construction` | Extend `MaterializedViewRewrite.apply` to query `getCurrentTableVersion` per base, construct row-level candidate, build `affected_identifiers`, build fresh-branch anti-join, add to `MVRewriteCandidatesNode` |
| `mv-row-level-rowid-dataflow` | Row-level leaves in `DifferentialPlanRewriter`; `resolveGroupingIdentifiers`; v1 restriction: row-preserving with joins → partition-level fallback |
| `mv-row-level-aggregating-expansion` | Case B (expanded `affected_identifiers` semi-join) in `visitAggregation` |
| `mv-row-level-classical-ivm-sum-count` | Classical-IVM path for SUM/COUNT: change-set scan → signed projection → `γ` → MERGE with column arithmetic. No base-table scan |
| `mv-row-level-refresh-rule` | `IncrementalRefreshRule` extensions: `rowLevelReplacementIsSafe`, `hasPartitionRefreshData` gate, `refreshBound` integration, `AUTOMATIC` candidate node |

**Tier 3 — Connector + Surface (can overlap with Tier 2)**

| Task | Description |
|---|---|
| `mv-row-level-iceberg-changeset-provider` | Implement `getCurrentTableVersion`, `getChangeSet`, `estimateChangeSetSize` on `IcebergAbstractMetadata`; backed by `IncrementalChangelogScan`; V2 tables return empty |
| `mv-row-level-session-properties` | Add `materialized_view_row_level_incremental_strategy` session property and `row_level_incremental_refresh` MV property |
| `mv-row-level-fallback-warnings` | `MATERIALIZED_VIEW_STITCHING_FALLBACK` extension + new `MATERIALIZED_VIEW_ROW_LEVEL_REJECTED_ON_COST` warning |
| `mv-row-level-cost-picker-integration` | Verify `SelectLowestCostMVRewrite` picks correctly across all three candidate types; verify row-count fallback when delta-size stats unavailable |

**Tier 4 — Validation**

| Task | Description |
|---|---|
| `mv-row-level-correctness-suite` | End-to-end tests covering the four correctness invariants: no duplicates, no missing rows, snapshot equivalence. Representative workloads: CDC fact table (SUM/COUNT aggregating MV), MV with holistic aggregations. Three-tier behaviour: V3 → row-level; V2 → partition-level; high-stale-fraction → full recompute |

### 12. End-to-End SPI Call Flow at Refresh

```
// 1. Read MV status (one call, three pieces of state)
status = metadata.getMaterializedViewStatus(mvName, TupleDomain.all())
recordedByBase = status.getRecordedBaseTableHandles()
changedByBase  = status.getChangedRowsPredicates()

// 2. For each base table:
currentHandle  = metadata.getTableHandle(session, baseName, Optional.empty())
recordedHandle = recordedByBase.get(baseName)
changed        = changedByBase.get(baseName)   // {dataDisjuncts, refreshBound}

// 3. Check row-level capability + extract versions:
pinnedVersionOpt   = metadata.getCurrentTableVersion(session, currentHandle)
recordedVersionOpt = metadata.getCurrentTableVersion(session, recordedHandle)

// 4. If both present: build row-level candidate
//    affected_identifiers = filtered base scan over dataDisjuncts
//    fresh_branch = ANTI JOIN(MV_storage, affected_identifiers)
//    delta_branch = Case B expansion semi-join
//    union = UNION ALL(fresh_branch, delta_branch)

// 5. estimateChangeSetSize for cost-picker cardinality
changeSetSize = metadata.estimateChangeSetSize(session, currentHandle,
                    recordedVersionOpt.get(), pinnedVersionOpt.get())
```

## Open Questions

These need a decision before the implementation is final but do not block starting the work.

**Q1 — Iceberg connector policy for V3 tables with known writer bugs**

During the V3 spec rollout, specific writer-side issues (e.g., `apache/iceberg#13232` — duplicate
`_row_id` after `add_files` from Parquet files already carrying `_row_id`) may produce tables
whose row lineage is unreliable.

Recommendation: detect known-bad writer versions via table metadata and return
`Optional.empty()` from `getCurrentTableVersion` as a safety valve. Decision needed from the
Iceberg connector team.

**Q2 — Fallback behaviour when row lineage is unavailable**

When a base table lacks row-level capability (`getCurrentTableVersion` returns empty):

| Option | Behaviour |
|---|---|
| (a) Silent fall-through | Degrade to partition-level invisibly |
| (b) Warning + fall-through (tentative recommendation) | Emit `MATERIALIZED_VIEW_STITCHING_FALLBACK`; aligns with PR #27816 plumbing |
| (c) Fail loudly | Only when `row_level_incremental_refresh = true`; MV demands row-level |

Decision needed from engine / policy owners.

**Q3 — Write-side capability story for MV storage tables**

If a connector supports change-tracking but not MERGE on the MV storage table:

| Option | Tradeoff |
|---|---|
| (a) Require MERGE (tentative recommendation) | Simplest; connectors mature enough for change-tracking almost certainly support MERGE |
| (b) Engine-internal DELETE+INSERT synthesis | Works without MERGE; maintains two write paths |

Note: Hive as MV storage is intentionally excluded — Hive MVs are deprecated.
Decision needed from engine owners.

## Other Approaches Considered

### Classical IVM Without Row Lineage (Rejected)

Earlier designs considered implementing classical IVM for SUM/COUNT using only the existing
partition-level machinery (no `_row_id`, no V3 requirement). This would have required
attributing every row in a changed partition to one of INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE,
which is not possible from Iceberg's `IncrementalAppendScan` (append-only). Full
`IncrementalChangelogScan` requires V3. Rejected: the benefit narrows to append-only tables, which
already work well with partition-level.

### Separate `getChangedRowsPredicate` Method (Rejected)

An earlier iteration added a parallel `getChangedRowsPredicate(table, since)` method. Rejected
because the engine always passes `since = recorded` (sourced from the same status object), so
per-base lazy computation provides no benefit. The cleaner shape is to upgrade
`partitionsFromBaseTables` in-place via two new fields and deprecate the original.

### Row Identity via User-Declared Primary Keys (Deferred)

For V2 Iceberg tables with user-declared PKs, row identity could be approximated from PK
columns. Deferred to v2: PK stability under schema evolution is a correctness hazard (schema
evolution can change or drop PK columns, invalidating row identities silently). V2 tables return
`Optional.empty()` in v1.

## Adoption Plan

**Impact on existing users**: None. All new SPI methods have default no-op / empty
implementations. Connectors that do not implement the new methods continue to use the
partition-level path unchanged. The row-level candidate is a third option added to the cost-based
picker; it does not displace the existing two.

**Feature flags**: The feature is gated by `materialized_view_row_level_incremental_strategy`
(session) and `row_level_incremental_refresh` (per-MV property). Both default to `auto` /
unset, which means the cost picker decides. Row-level fires only when it is cheaper.

**Rollout path**: V3-capable Iceberg tables automatically get row-level capability once
`IcebergAbstractMetadata` implements the SPI. `auto` mode means row-level fires when cheaper;
per-MV `true` enables loud-failure mode for users who treat MV freshness as a hard SLA.

**SPI compatibility**: The `MaterializedViewStatus` constructor chain maintains backward
compatibility — existing two-argument and three-argument constructors delegate to the new
five-argument constructor with empty maps for the new fields.

**Migration from partition-level refresh**: No migration required. Partition-level refresh
continues to work unchanged for V2 base tables and any connector that does not implement the new
SPI. V3 tables automatically gain row-level capability; the cost picker decides whether to use it.

**Documentation**: The new session property, MV property, `system.changes` TVF, and
`ChangeKindEnumType` all require documentation updates. The connector SPI implementation guide
(from RFC-0016) needs a new section describing the row-level change-tracking contract and the
pairing obligations.

**What is out of scope for this RFC** (addressed independently):
- Row-preserving MV support (single-base, no joins)
- Classical IVM for AVG/STDDEV/MIN/MAX
- CTE sharing for `affected_identifiers`
- Distributed `system.changes` execution
- Delta Lake / JDBC connector row-level implementations

## Test Plan

### Unit Tests (already present in implementation branch)

- `TestChangedRowsPredicate` — immutability and `empty()` factory.
- `TestMaterializedViewStatus` — existing constructors leave row-level state empty; new
  constructor copies maps defensively.
- `TestChangeKindEnumType` — enum values correct; registered type resolves by name.

### Integration Tests

End-to-end tests covering the four correctness invariants:

1. **No duplicates** — a stitched query over a partially stale V3 MV produces the same rows as
   a full recompute with no duplicates.
2. **No missing rows** — deleted rows are excluded from the fresh branch and their affected
   groups are recomputed in the delta branch.
3. **Snapshot equivalence** — stitched result equals full recompute at each base's pinned
   snapshot.
4. **Monotonic freshness** — a base-table commit during execution does not change the plan or
   result.

**Representative workloads**:
- CDC fact table (SUM/COUNT aggregating MV) — validates classical IVM path.
- MV with holistic aggregations (MEDIAN, COUNT DISTINCT) — validates Case B expansion.
- Three-tier behaviour: V3 base → row-level; V2 base → partition-level; high-stale-fraction
  base → full recompute (cost picker).
- Mixed V3 + V2 bases in a single MV — row-level leaf for V3, partition-level for V2.

## References

| Item | Link |
|---|---|
| M3 milestone | [#4516](https://github.com/prestodb/presto/issues/4516) |
| RFC-0016 (MV architecture) | [RFC-0016-materialized-views.md](RFC-0016-materialized-views.md) |
| Tier 0 SPI PR | [prestodb/presto#28451](https://github.com/prestodb/presto/pull/28451) |
| Full implementation branch | [Nandakumar-Balagopal/presto@mv-row-level-incremental-refresh](https://github.com/Nandakumar-Balagopal/presto/tree/mv-row-level-incremental-refresh) |
| PR #27774 — Bounded MV refresh | [prestodb/presto#27774](https://github.com/prestodb/presto/pull/27774) |
| PR #27816 — Stitching and refresh fallback warnings | [prestodb/presto#27816](https://github.com/prestodb/presto/pull/27816) |
| PR #27820 — Cost-based stitching + `MVRewriteCandidatesNode` | [prestodb/presto#27820](https://github.com/prestodb/presto/pull/27820) |
| PR #27733 — JOIN support in MV query optimizer | [prestodb/presto#27733](https://github.com/prestodb/presto/pull/27733) |
| PR #27677 — HAVING support in MV query rewrite | [prestodb/presto#27677](https://github.com/prestodb/presto/pull/27677) |
| PR #27917 — MV optimizer applied to CTAS and INSERT bodies | [prestodb/presto#27917](https://github.com/prestodb/presto/pull/27917) |
| Iceberg V3 row lineage spec | [https://iceberg.apache.org/spec/#row-lineage](https://iceberg.apache.org/spec/#row-lineage) |
