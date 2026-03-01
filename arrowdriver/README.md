# ArrowDriver Architecture

This document explains how the `benchtop/arrowdriver` works, from first principles.

It is intentionally written for engineers who may not have deep database internals background.

---

## 1. What problem this driver solves

`ArrowDriver` is a storage engine used by Benchtop/GRIP to store and query graph-backed JSON records at high throughput.

At a high level, it needs to do two things well:

1. Write lots of JSON-like documents to disk safely.
2. Read/filter those documents very quickly for common query patterns.

In your workload, the hottest operations are things like:

- `V().hasLabel("Observation")`
- `V().has(...)`
- `V().hasLabel(...).has(...)`
- projected reads (`fields(["_id", ...])`)

The driver is designed so common top-level filters can avoid expensive full JSON decoding when possible.

---

## 2. Mental model: table engine + index engine

The driver is easier to understand if you think of it as two cooperating subsystems:

1. **Data files (Arrow sections)**
- Store row payloads and selected materialized columns.
- Optimized for sequential, batch-oriented reads.

2. **Index files (bbolt)**
- Map row IDs and indexed field values to physical row locations.
- Optimized for fast key lookup and scanning index ranges.

The query path normally uses both:

- index first to identify candidate rows,
- then Arrow section read for payload/fields.

---

## 3. Core types and responsibilities

### `ArrowDriver` (`driver.go`)

Think of this as the "database instance" manager.

Responsibilities:

- discovers all tables on startup
- manages table name <-> numeric table ID mapping
- opens/closes table handles
- stores driver-global metadata in `driver.meta`
- routes high-level operations to specific tables

It does **not** hold row data itself; it orchestrates table objects.

### `ArrowTable` (`table.go`)

Think of this as one physical table's storage and indexes.

Responsibilities:

- write rows into new Arrow section files
- maintain row ID and field indexes in `.idx` bbolt file
- perform row fetch/scan/filter/projection operations
- maintain caches for repeated reads
- expose both map-based and raw-payload read interfaces

Most throughput behavior is decided at this level.

---

## 4. On-disk layout (what files exist and why)

Base directory:

- `<graph>/ARROW_TABLES/`

For each table `<table>`:

1. `<table>.idx` (bbolt)
- key-value index database for this table.
- stores row ID -> location mapping.
- stores field index entries for fast `has(field == value)` operations.

2. `<table>.<section>.arrow` (Arrow IPC stream)
- each file is a "section" of rows.
- sectioning avoids one huge monolithic file and supports append-by-section writes.

Driver-global file:

3. `driver.meta` (bbolt)
- table registry and next table ID state.
- lets the driver recover table identity consistently after restart.

Why this split exists:

- bbolt is excellent at point/range key lookup.
- Arrow is excellent at dense columnar I/O and batched section reads.
- combining both gives practical speed for mixed workloads.

---

## 5. Data model inside a table row

Each logical row has:

- `_id`: stable row identifier
- `_data`: raw JSON payload string (full document body)
- optional materialized top-level columns

Materialized columns exist to accelerate common filter/projection patterns.

Example conceptually:

- full JSON document stored in `_data`
- top-level `status`, `auth_resource_path` also stored in typed Arrow columns

This lets the driver avoid parsing full JSON for simple top-level queries.

---

## 6. Row location and indirection (`RowLoc`)

Rows are addressed through `benchtop.RowLoc`:

- `TableId`
- `Section`
- `Offset`
- (and bookkeeping fields like `Size`, `Index`)

Why indirection matters:

- indexes point to `RowLoc`, not payload bytes.
- the same logical row ID lookup can quickly locate data in Arrow sections.
- many code paths can batch by section for better locality.

---

## 7. Write path in detail

Primary entry point: `AddRows` (`table_ops.go`)

Step-by-step:

1. Reserve next section number.
2. Convert incoming rows into section representation.
3. Write Arrow section file.
4. Update `.idx` buckets:
- row ID -> encoded `RowLoc`
- field index entries for indexed fields
5. Update metadata/indexed-field bookkeeping.
6. Invalidate caches affected by the write.

Important design tradeoff:

- writes are append-like at section granularity (good throughput),
- but require index updates (necessary for fast reads).

Bulk load (`driver.go`) uses larger batches and post-load compaction behavior to reduce section fragmentation.

---

## 8. Read paths (from cheapest to most expensive)

### 8.1 `ScanId`

Cheapest path.

- Reads IDs from index structures.
- Avoids full payload decode.
- used when caller only needs identity.

### 8.2 projected reads (`ScanDocProjected`, `GetRowsProjected`)

- read only requested top-level fields when possible.
- avoids full document materialization.
- falls back to full decode if projection is not pushdown-safe (nested paths).

### 8.3 raw payload reads (`ScanDocRaw`, `GetRowsRawPayload`)

- return raw JSON payloads (`_data`) plus ID.
- avoids `map[string]any` decode for each row.
- caller can defer or skip materialization.

These are optional capabilities and should be used via interface checks.

### 8.4 full map reads (`ScanDoc`, `GetRows`)

Most expensive path.

- decode payload to Go maps for each row.
- needed for generic operations that require map semantics.
- becomes expensive at large row counts due to allocations and GC pressure.

---

## 9. Filtering strategy (how the driver chooses a plan)

Filtering code is primarily in `table_filtering.go`.

The engine tries to use the cheapest strategy that is valid for a given filter.

### Strategy A: indexed top-level conjunction

For filters like:

- `status == "final"`
- `auth_resource_path in [...]`

the driver can intersect index postings and jump directly to candidate rows.

This is usually the fastest approach for selective filters.

### Strategy B: column/materialized scan

If columns are materialized and filter is top-level-compatible, it can evaluate using column reads without full payload parse.

### Strategy C: raw payload lookup

For nested/deep paths where index/column pushdown is not enough, raw JSON lookup (`sonic.GetFromString`) can evaluate conditions without full map unmarshal.

### Strategy D: full decode fallback

When none of the above safely applies, decode full rows and evaluate in generic mode.

This is the safety net path; correctness-first, slower.

---

## 10. Scan section mechanics and batching

Large scans are grouped by section for locality.

Typical pattern:

1. collect candidate offsets by section
2. read that section once
3. return rows at requested offsets in deterministic order

Why this matters:

- less random I/O than row-by-row fetch
- better CPU cache behavior
- easier worker parallelism by section

`table_sections.go` contains section reader helpers for:

- full row decode
- projected field extraction
- raw payload-by-offset scanning

---

## 11. Caching model

`ArrowTable` keeps several caches to avoid repeated decode/index work:

- column cache
- section row cache
- value-index cache
- row ordinal cache

These caches are local and opportunistic.

They are invalidated after writes/deletes/reindex so reads never serve stale state.

A useful practical interpretation:

- first query after cold start is often slower,
- subsequent similar queries can be significantly faster.

---

## 12. Concurrency model

Reads:

- parallel workers often process per-section tasks
- worker count is bounded (`NumCPU()` with caps)

Writes:

- guarded by table locks
- index updates happen inside bbolt transactions

This design prioritizes read throughput while preserving write correctness.

---

## 13. Optional capabilities and compatibility contract

`arrowdriver` fully implements required `benchtop` interfaces.

It also exposes optional performance capabilities:

- `ScanDocRaw`
- `GetRowsRawPayload`

Callers (e.g., GRIP layers) must treat these as optional:

1. check capability via interface assertion,
2. use fast path if available,
3. otherwise fallback to standard `ScanDoc`/`GetRows`.

This is the key reason other drivers continue to work unchanged.

---

## 14. Throughput characteristics in real workloads

Usually fast:

- selective indexed top-level filters
- simple projections
- raw-payload pass-through pipelines

Usually expensive:

- huge full-row scans that must materialize maps
- deep nested filters mixed with heavy transforms (`render`, `unwind`, grouping)
- workloads with many allocations triggering GC variability

If you see runtime variance, common causes are:

- cold cache vs warm cache
- GC/scavenger timing
- profiler overhead during measured runs
- OS scheduling/noisy neighbor effects

---

## 15. Practical tuning checklist

When tuning this driver, validate in this order:

1. **Plan choice correctness**
- confirm query takes indexed or projected/raw path when expected.

2. **Materialization avoidance**
- avoid full `map[string]any` decode unless pipeline semantics require it.

3. **Section locality**
- ensure reads are grouped by section, not random row fetch loops.

4. **Cache behavior**
- compare warm and cold runs separately.

5. **Benchmark discipline**
- collect medians/p95 over many runs.
- avoid drawing conclusions from one profiled run.

---

## 16. File-by-file guide

- `driver.go`
  - driver lifecycle, table discovery, table ID metadata, bulk pathways
- `table.go`
  - table struct, cache fields, constants
- `table_meta.go`
  - table creation/open and metadata persistence
- `table_ops.go`
  - add/delete/get APIs, projected and raw batched row fetch
- `table_scan.go`
  - map-based scan APIs (`ScanDoc`, `ScanFull`, etc.)
- `table_scan_raw.go`
  - raw payload scan API (`ScanDocRaw`)
- `table_filtering.go`
  - filter extraction, optimizer decisions, deep/raw evaluation
- `table_sections.go`
  - Arrow IPC section read/write helpers
- `table_index.go`
  - index build/update/read paths
- `table_row_ordinal_cache.go`
  - ordinal cache invalidation helpers
- `table_value_cache.go`
  - auxiliary cached lookup helpers

---

## 17. Non-goals and boundaries

This driver is not trying to be:

- a full SQL query planner,
- a distributed storage engine,
- a transactional multi-writer database.

It is a high-performance embedded table backend optimized for Benchtop/GRIP access patterns.

---

## 18. Summary

`ArrowDriver` is a hybrid design:

- Arrow sections for dense data reads,
- bbolt indexes for fast key/filter lookup,
- optional raw-payload interfaces to reduce decode overhead,
- strict fallback behavior to preserve compatibility with other drivers and generic callers.

If you remember one core idea, remember this:

- **Performance comes from avoiding unnecessary full JSON materialization.**

