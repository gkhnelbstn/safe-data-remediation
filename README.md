# safe-data-remediation

`safe-fix`: a small Python CLI that runs one bulk `UPDATE` in PostgreSQL so that it can be checked before it commits and undone after.

This is a public, generated-data version of how I fixed 652,513 production ERP records at a logistics company (details anonymized). The rule was simple: no bulk fix runs unless we know exactly which rows it touches, have a backup of them, and have a tested way back.

| | |
| --- | --- |
| **Scope** | Rows are locked once, and every later step works on exactly those keys. |
| **Gates** | Locked rows must equal `expect_rows`; updated rows must equal locked rows; a `verify` query must return 0. Any failure rolls the whole fix back. |
| **Backup** | A CSV of the full rows and a generated rollback SQL script, written and fsynced *before* the UPDATE runs. |
| **Rollback** | One transaction. It restores only rows that still hold the fixed value, and refuses to commit if anything changed since. |
| **Audit** | Every run is recorded in `remediation.audit_log`: applied, dry run, aborted, rolled back. |
| **Result** | On 2,000,000 rows, 293,147 rows are fixed in 4.0 s and rolled back in 3.4 s. The table checksum after rollback equals the one before the fix. |

## A fix, end to end

The example table `public.shipments` has 2M generated rows. Two carriers changed their feed on 2025-06-01 and started sending weights in grams. Real shipments weigh 5 to 2,000 kg, so their weights since then are 1,000 times too large.

The fix is a TOML file, [`examples/shipment-weights-in-grams.toml`](examples/shipment-weights-in-grams.toml):

```toml
name = "shipment-weights-in-grams"
table = "public.shipments"
where = "carrier_id in (3, 5) and shipped_on >= date '2025-06-01' and weight_kg > 5000"
set = { weight_kg = "round(weight_kg / 1000, 3)" }
expect_rows = 293147
verify = """
select count(*) from public.shipments
where carrier_id in (3, 5) and shipped_on >= date '2025-06-01' and weight_kg > 5000
"""
```

**1. Make the example table and take a checksum.** The checksum is an md5 over every row in key order.

```
$ safe-fix demo --rows 2000000
public.shipments: 2,000,000 rows, 293,147 with weights in grams
$ safe-fix checksum public.shipments
47b1e11990137aedee9c6d5334c007a3
```

**2. Dry run.** It runs the real code path (lock, gates, backup, rollback script, UPDATE, verify), then rolls back. `expect_rows` in the spec comes from this count.

```
$ safe-fix apply examples/shipment-weights-in-grams.toml --dry-run
DRY RUN: shipment-weights-in-grams on shipments
locked 293,147 rows (spec expects 293,147)
backup and rollback script written to runs/shipment-weights-in-grams-20261120T214213256180Z-dry-run
updated 293,147 rows; all gates passed
dry run: rolled back, nothing was changed
$ safe-fix checksum public.shipments
47b1e11990137aedee9c6d5334c007a3
```

**3. Apply.**

```
$ safe-fix apply examples/shipment-weights-in-grams.toml
shipment-weights-in-grams on shipments
locked 293,147 rows (spec expects 293,147)
backup and rollback script written to runs/shipment-weights-in-grams-20261120T214220886347Z
updated 293,147 rows; all gates passed
committed in 4.03s
$ safe-fix checksum public.shipments
f5e42d2aaea6911603fd3966aa0bc3ae
```

The run directory holds everything needed to explain or undo the fix:

| File | Size | What it is |
| --- | ---: | --- |
| `backup.csv` | 15.8 MB | The 293,147 rows as they were, all columns |
| `rollback.sql` | 11.4 MB | A self-contained script with old and new values as server-quoted literals |
| `manifest.json` | 450 B | Counts, timings and the sha256 of the spec, backup and script |

The start of `rollback.sql`:

```sql
begin;
set local lock_timeout = '5s';
create temp table safe_fix_rollback ("shipment_id" bigint, "old_weight_kg" numeric(12,3), "new_weight_kg" numeric(12,3)) on commit drop;
insert into safe_fix_rollback values
('827402', '184380.000', '184.380'),
('827404', '342760.000', '342.760'),
...
```

It ends with a guarded UPDATE in a `DO` block. A row is restored only if it still holds the new value, and the script raises an exception, so nothing commits, if it restored fewer rows than the fix changed.

**4. Roll back** and compare checksums.

```
$ safe-fix rollback runs/shipment-weights-in-grams-20261120T214220886347Z
safe-fix rollback: restored 293147 of 293147 rows, 0 changed since the fix
$ safe-fix checksum public.shipments
47b1e11990137aedee9c6d5334c007a3
```

The checksum matches step 1. The audit log shows the three runs:

```
 id | status      | rows_locked | rows_changed
----+-------------+-------------+-------------
  1 | dry_run     |      293147 |       293147
  2 | applied     |      293147 |       293147
  3 | rolled_back |      293147 |       293147
```

`rollback.sql` also runs without this tool: `psql -v ON_ERROR_STOP=1 -f rollback.sql`.

## The design decisions that make it safe

- **Lock once, then work on keys.** The scope is `SELECT <primary key> ... FOR UPDATE` into a temp table. The count gate, the backup, the rollback script and the UPDATE all join on those keys. They do not evaluate `where` again, so a row that starts matching halfway through is not touched, and no locked row can change underneath the fix.
- **Backup before change.** The CSV and the rollback script are written, fsynced and hashed before the UPDATE runs. If the disk is full, the fix aborts with nothing changed.
- **The server quotes the values.** Rollback literals come from PostgreSQL's `quote_nullable`, so NULLs, quotes, backslashes, newlines, non-ASCII text and numeric scale come back exactly. A test checks all of these.
- **Rollback is all or nothing.** If a single fixed row was edited after the fix, the rollback aborts and says how many rows changed. You decide whether to accept the skips (`--allow-skips`), rather than the script deciding for you.
- **Tamper check.** `rollback` refuses to run a script whose sha256 does not match the one recorded when the fix ran.
- **Aborted runs are recorded too.** The audit row for an aborted fix is written on a separate connection, so it is not rolled back together with the fix.
- **Refuse unsafe input.** Tables without a primary key are refused, and so are fixes that would change a key column. `lock_timeout` and `statement_timeout` are always set.

## Scope and limits

- **UPDATE only.** A delete fix would need an INSERT-based rollback. That is not built.
- **Trusted SQL.** `where`, `set` and `verify` in a spec are SQL written by the operator, and they are trusted. Identifiers are quoted; these values are not. A spec is a reviewed change, like a migration.
- **PostgreSQL only.**
- **Rollback scripts grow with the scope:** about 40 bytes per row per fixed column. 293k rows give an 11 MB script.

## Run it

Requirements: Docker and [uv](https://docs.astral.sh/uv/).

```sh
cp .env.example .env        # set a password
docker compose up -d --wait # PostgreSQL 17
uv sync
set -a; . ./.env; set +a    # safe-fix uses the standard PG* variables
uv run safe-fix demo --rows 2000000
uv run safe-fix apply examples/shipment-weights-in-grams.toml --dry-run
```

Timings are from an Intel Core i5-12500H laptop with PostgreSQL in Docker Desktop.

## Tests and CI

`tests/test_fix.py` runs against a real PostgreSQL. Each test pins one claim from this README:
- A dry run changes nothing but writes all the files.
- Apply followed by rollback restores the table checksum.
- A wrong `expect_rows` aborts with nothing changed.
- A failing verify query aborts with nothing changed.
- Rollback refuses when a row changed after the fix, and restores the rest with `--allow-skips`.
- A modified rollback script is refused.
- Awkward values (NULL, quotes, backslashes, newlines, non-ASCII) round-trip.
- A table without a primary key is refused.

GitHub Actions runs lint and the tests against a PostgreSQL service container. It then runs the walkthrough above at 2M rows and checks the checksums at each step.

## License

MIT
