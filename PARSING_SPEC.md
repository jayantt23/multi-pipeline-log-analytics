# PARSING_SPEC

**Status:** FROZEN. Every backend (MongoDB Phase 1; Pig / Hive / MapReduce Phase 2) must implement these rules byte-for-byte. Equivalence across pipelines is verified against this spec.

The NASA HTTP access logs follow the Common Log Format with a few quirks (some lines have `bytes = "-"`, some have `status = "-"`, a small fraction is malformed).

---

## 1. Master regex

POSIX-compatible. Used identically in every engine (`$regexFind` in Mongo, `regexp_extract` in Hive, `REGEX_EXTRACT_ALL` in Pig, `Pattern.compile` in MR).

```
^(\S+) \S+ \S+ \[([^\]]+)\] "([^"]*)" (\d{3}|-) (\d+|-)$
```

Capture groups:

| #   | Field           | Notes                                                |
| --- | --------------- | ---------------------------------------------------- |
| 1   | `host`          | first whitespace-delimited token                     |
| 2   | `timestamp_raw` | inside `[...]`, e.g. `01/Jul/1995:00:00:01 -0400`    |
| 3   | `request_raw`   | inside `"..."`, e.g. `GET /history/apollo/ HTTP/1.0` |
| 4   | `status_raw`    | 3-digit HTTP status, or literal `-`                  |
| 5   | `bytes_raw`     | non-negative integer, or literal `-`                 |

If the outer regex does not match, the record is **malformed** (see §6).

---

## 2. Timestamp

Format: `%d/%b/%Y:%H:%M:%S %z` (e.g. `01/Jul/1995:00:00:01 -0400`).

- `log_date` and `log_hour` are derived **in the timestamp's own timezone offset**. Do **not** convert to UTC. The NASA logs are all in `-0400`/`-0500`, but the rule is local-offset to keep semantics identical across engines whose default tz handling differs.
- `log_date` → `DATE` (e.g. `1995-07-01`).
- `log_hour` → `INT` in `[0, 23]`.

---

## 3. Request split

Split `request_raw` on whitespace into at most 3 tokens:

| Tokens | `http_method` | `resource_path` | `protocol_version` |
| ------ | ------------- | --------------- | ------------------ |
| 0      | NULL          | NULL            | NULL               |
| 1      | token[0]      | NULL            | NULL               |
| 2      | token[0]      | token[1]        | NULL               |
| ≥3     | token[0]      | token[1]        | token[2]           |

A short request line is **NOT** malformed — only NULL the missing fields.

---

## 4. Bytes

| `bytes_raw`    | `bytes_transferred` |
| -------------- | ------------------- |
| `-`            | `0`                 |
| numeric string | parse as `BIGINT`   |

---

## 5. Status

| `status_raw`                 | Action                  |
| ---------------------------- | ----------------------- |
| 3 digits (e.g. `200`, `404`) | parse as `INT`          |
| `-`                          | record is **MALFORMED** |

---

## 6. Malformed records

A record is malformed iff:

1. The master regex (§1) does not match, **OR**
2. The matched `status_raw` is `-`.

Malformed records are **counted** (`malformed_count` in `runs`) and **not** written to any side store. They never reach Q1/Q2/Q3 aggregation.

---

## 7. Ordinal

`ordinal` is the 1-indexed position of the record across the input files concatenated in **lexicographic order**:

```
NASA_access_log_Jul95   → ordinals 1 .. |Jul95|
NASA_access_log_Aug95   → ordinals |Jul95|+1 .. |Jul95|+|Aug95|
```

Each engine must use its native primitive to materialise this:

- MongoDB: explicit monotonically increasing `_id = 1..N` set during ingestion.
- Hive: `ROW_NUMBER() OVER (ORDER BY <stable_input_order>)`.
- Pig: `RANK <relation> BY ... DENSE` over the same stable order.
- MapReduce: line-offset emitted by the input format, then a single-reducer pass to assign `1..N`.

Ordinals are computed over **all** input lines (including malformed) so that adding/removing malformed lines does not shift batch boundaries.

---

## 8. Batch ID

```
batch_id = floor((ordinal - 1) / N) + 1
```

where `N` is the user-supplied `--batch-size`. Batch IDs start at 1. The final batch may have `< N` records but is still counted. `num_batches = max(batch_id over parsed records)` and `avg_batch_size = total_records / num_batches` (per the project statement).

---

## 9. Q1 — Daily Traffic Summary

`GROUP BY (log_date, status_code) → SUM(1) AS request_count, SUM(bytes_transferred) AS total_bytes`.

Output ordering: `log_date ASC, status_code ASC`.

---

## 10. Q2 — Top Requested Resources

`GROUP BY resource_path → SUM(1) AS request_count, SUM(bytes_transferred) AS total_bytes, COUNT(DISTINCT host) AS distinct_host_count`.

**Distinct hosts must be exact** (not approximate). MongoDB stages a `(resource_path, host)` distinct-pairs collection and counts; Hive uses `COUNT(DISTINCT host)`; Pig uses `DISTINCT` then `COUNT_STAR`.

Tie-break:

```
ORDER BY request_count DESC, resource_path ASC
LIMIT 20
```

`rank` = 1..20.

---

## 11. Q3 — Hourly Error Analysis

For records with `status_code BETWEEN 400 AND 599`:

```
GROUP BY (log_date, log_hour) →
    SUM(1) AS error_request_count,
    COUNT(DISTINCT host) AS distinct_error_hosts
```

Joined with the same-key total request count from the full parsed set:

```
total_request_count = COUNT(*) WHERE same (log_date, log_hour)
error_rate          = error_request_count / total_request_count   (NUMERIC, 6 decimal places)
```

Output ordering: `log_date ASC, log_hour ASC`.

If `total_request_count = 0` for a `(log_date, log_hour)` that has zero errors, the row is omitted (Q3 is keyed off the error set).

---

## 12. Summary of NULL / 0 / malformed handling

| Input quirk                 | Treatment                                         |
| --------------------------- | ------------------------------------------------- |
| `bytes_raw = "-"`           | `bytes_transferred = 0`, record is valid          |
| `status_raw = "-"`          | record is **malformed**, dropped from aggregation |
| Short request (`<3` tokens) | missing fields are `NULL`, record is valid        |
| Outer regex fails           | record is **malformed**, dropped from aggregation |
| Empty line                  | outer regex fails → malformed                     |
