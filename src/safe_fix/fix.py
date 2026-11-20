"""safe-fix: run one bulk UPDATE in PostgreSQL so that it can be checked and undone.

A fix is a small TOML file (see ``examples/``). ``apply`` does this, in one transaction:

1. Lock the rows in scope once (``SELECT key ... FOR UPDATE`` into a temp table). Every later
   step works on exactly these keys, so rows that start matching mid-run are not touched.
2. Gate: the locked count must equal ``expect_rows`` from the spec.
3. Write a CSV backup of the full rows and a rollback SQL script, fsync both and hash them.
   This happens before the UPDATE, so a failed write means nothing was changed.
4. UPDATE by joining on the locked keys. Gate: updated rows must equal locked rows.
5. Gate: the spec's ``verify`` query must return 0.
6. Record the run in ``remediation.audit_log`` and commit. Any failed gate rolls back.

``--dry-run`` runs the same steps, writes the same files, then rolls back.
``rollback`` runs the generated script, which restores only rows that still hold the fixed
value and refuses to commit if it restored fewer rows than the fix changed.

The ``where``, ``set`` and ``verify`` values in a spec are trusted operator SQL.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
import tomllib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from psycopg import sql

ROLLBACK_BATCH = 1_000
_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_REQUIRED = {"name", "table", "set", "where", "expect_rows"}
_OPTIONAL = {"verify", "lock_timeout", "statement_timeout"}

AUDIT_DDL = """
create schema if not exists remediation;
create table if not exists remediation.audit_log (
    id               bigint generated always as identity primary key,
    fix_name         text        not null,
    status           text        not null check (status in
                         ('applied', 'dry_run', 'aborted', 'rolled_back', 'rollback_aborted')),
    db_user          text        not null default current_user,
    started_at       timestamptz not null,
    finished_at      timestamptz not null default now(),
    table_name       text,
    rows_locked      bigint,
    rows_changed     bigint,
    run_dir          text,
    spec_sha256      text,
    backup_sha256    text,
    rollback_sha256  text,
    message          text
);
"""


class FixAborted(Exception):
    """A gate failed; the transaction was rolled back and nothing was changed."""


@dataclass
class Spec:
    name: str
    table: str
    set: dict[str, str]
    where: str
    expect_rows: int
    verify: str | None = None
    lock_timeout: str = "5s"
    statement_timeout: str = "30min"
    sha256: str = ""


def load_spec(path: str | Path) -> Spec:
    raw = Path(path).read_bytes()
    data = tomllib.loads(raw.decode())
    if missing := _REQUIRED - data.keys():
        raise ValueError(f"spec is missing {sorted(missing)}")
    if unknown := data.keys() - _REQUIRED - _OPTIONAL:
        raise ValueError(f"spec has unknown keys {sorted(unknown)}")
    if not _NAME.match(data["name"]):
        raise ValueError("name must be lowercase letters, digits and dashes")
    if not isinstance(data["expect_rows"], int) or data["expect_rows"] < 1:
        raise ValueError("expect_rows must be a positive integer (take it from a dry run)")
    if not data["set"] or not all(isinstance(v, str) for v in data["set"].values()):
        raise ValueError("set must map column names to SQL expressions")
    return Spec(**data, sha256=hashlib.sha256(raw).hexdigest())


@dataclass
class Target:
    name: sql.Composable  # schema-qualified, quoted by the server
    label: str
    keys: list[tuple[str, str]]  # (column, type) of the primary key
    cols: list[tuple[str, str]]  # (column, type) of the columns the fix sets


def describe(cur: psycopg.Cursor, spec: Spec) -> Target:
    row = cur.execute("select to_regclass(%s)::oid, to_regclass(%s)::text", (spec.table,) * 2)
    oid, label = row.fetchone()
    if oid is None:
        raise FixAborted(f"table {spec.table} does not exist")
    keys = cur.execute(
        """select a.attname, format_type(a.atttypid, a.atttypmod)
           from pg_index i
           cross join lateral unnest(i.indkey::int2[]) with ordinality k(attnum, ord)
           join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.attnum
           where i.indrelid = %s and i.indisprimary
           order by k.ord""",
        (oid,),
    ).fetchall()
    if not keys:
        raise FixAborted(f"{label} has no primary key; refusing to fix rows that cannot be named")
    types = dict(
        cur.execute(
            """select attname, format_type(atttypid, atttypmod) from pg_attribute
               where attrelid = %s and attnum > 0 and not attisdropped""",
            (oid,),
        ).fetchall()
    )
    if unknown := set(spec.set) - types.keys():
        raise FixAborted(f"{label} has no column(s) {sorted(unknown)}")
    if changed_keys := set(spec.set) & {k for k, _ in keys}:
        raise FixAborted(f"refusing to change primary key column(s) {sorted(changed_keys)}")
    cols = [(c, types[c]) for c in spec.set]
    return Target(sql.SQL(label), label, keys, cols)


def _ids(names: Iterable[str], prefix: str = "") -> sql.Composable:
    return sql.SQL(", ").join(
        sql.SQL(prefix) + sql.Identifier(n) if prefix else sql.Identifier(n) for n in names
    )


def _key_join(left: str, right: str, keys: list[tuple[str, str]]) -> sql.Composable:
    return sql.SQL(" and ").join(
        sql.SQL("{}.{} = {}.{}").format(
            sql.Identifier(left), sql.Identifier(k), sql.Identifier(right), sql.Identifier(k)
        )
        for k, _ in keys
    )


def _write(path: Path, chunks: Iterable[bytes]) -> str:
    """Write, fsync and hash a file; the hash goes into the manifest and the audit log."""
    digest = hashlib.sha256()
    with open(path, "wb") as f:
        for chunk in chunks:
            f.write(chunk)
            digest.update(chunk)
        f.flush()
        os.fsync(f.fileno())
    return digest.hexdigest()


def _backup_csv(conn: psycopg.Connection, t: Target) -> Iterable[bytes]:
    query = sql.SQL(
        "copy (select t.* from {} t join fix_scope s on {} order by {}) "
        "to stdout with (format csv, header)"
    ).format(t.name, _key_join("t", "s", t.keys), _ids((k for k, _ in t.keys), "t."))
    with conn.cursor().copy(query) as copy:
        for data in copy:
            yield bytes(data)


def _rollback_sql(conn: psycopg.Connection, spec: Spec, t: Target, rows: int) -> Iterable[bytes]:
    """A self-contained script: old and new values as server-quoted literals, then a guarded
    UPDATE in a DO block that aborts if any row no longer holds the fixed value."""
    key_names = [k for k, _ in t.keys]
    col_names = [c for c, _ in t.cols]
    temp_cols = (
        [sql.SQL("{} {}").format(sql.Identifier(k), sql.SQL(ty)) for k, ty in t.keys]
        + [sql.SQL("{} {}").format(sql.Identifier("old_" + c), sql.SQL(ty)) for c, ty in t.cols]
        + [sql.SQL("{} {}").format(sql.Identifier("new_" + c), sql.SQL(ty)) for c, ty in t.cols]
    )
    literals = (
        [sql.SQL("quote_nullable(t.{})").format(sql.Identifier(k)) for k in key_names]
        + [sql.SQL("quote_nullable(t.{})").format(sql.Identifier(c)) for c in col_names]
        + [sql.SQL("quote_nullable(({}))").format(sql.SQL(spec.set[c])) for c in col_names]
    )
    restore = sql.SQL("update {} t set {} from safe_fix_rollback r where {} and {}").format(
        t.name,
        sql.SQL(", ").join(
            sql.SQL("{} = r.{}").format(sql.Identifier(c), sql.Identifier("old_" + c))
            for c in col_names
        ),
        _key_join("t", "r", t.keys),
        sql.SQL(" and ").join(
            sql.SQL("t.{} is not distinct from r.{}").format(
                sql.Identifier(c), sql.Identifier("new_" + c)
            )
            for c in col_names
        ),
    )

    def s(q: sql.Composable) -> str:
        return q.as_string(conn)

    yield (
        f"-- safe-fix rollback for '{spec.name}' on {t.label}\n"
        f"-- generated {datetime.now(UTC):%Y-%m-%d %H:%M:%SZ}, {rows} rows\n"
        "-- Run: safe-fix rollback <run dir>   or   psql -v ON_ERROR_STOP=1 -f rollback.sql\n"
        "-- Restores only rows that still hold the fixed value. If any row changed since the\n"
        "-- fix, the script aborts; set safe_fix.allow_skips = on to restore the rest anyway.\n"
        "begin;\n"
        f"set local lock_timeout = '{spec.lock_timeout}';\n"
        f"create temp table safe_fix_rollback ({s(sql.SQL(', ').join(temp_cols))})"
        " on commit drop;\n"
    ).encode()

    values = conn.cursor(name="safe_fix_rollback_values")
    values.itersize = 10_000
    values.execute(
        sql.SQL(
            "select '(' || concat_ws(', ', {}) || ')' from {} t join fix_scope s on {} order by {}"
        ).format(
            sql.SQL(", ").join(literals),
            t.name,
            _key_join("t", "s", t.keys),
            _ids(key_names, "t."),
        )
    )
    for i, (line,) in enumerate(values):
        head = "insert into safe_fix_rollback values\n" if i % ROLLBACK_BATCH == 0 else ""
        last = i + 1 == rows or (i + 1) % ROLLBACK_BATCH == 0
        yield f"{head}{line}{';' if last else ','}\n".encode()
    values.close()

    yield (
        "analyze safe_fix_rollback;\n"
        "do $safe_fix$\n"
        "declare\n"
        f"    expected constant bigint := {rows};\n"
        "    restored bigint;\n"
        "    allow_skips boolean := coalesce("
        "nullif(current_setting('safe_fix.allow_skips', true), ''), 'off')::boolean;\n"
        "begin\n"
        f"    {s(restore)};\n"
        "    get diagnostics restored = row_count;\n"
        "    raise notice 'safe-fix rollback: restored % of % rows, % changed since the fix',"
        " restored, expected, expected - restored;\n"
        "    if restored < expected and not allow_skips then\n"
        "        raise exception 'rollback restored only % of % rows; nothing was changed. "
        "Set safe_fix.allow_skips = on to restore the rest', restored, expected;\n"
        "    end if;\n"
        "end\n"
        "$safe_fix$;\n"
        "commit;\n"
    ).encode()


def _audit(status: str, **fields) -> None:
    """Write an audit row on its own connection, so it survives a rolled-back fix."""
    with psycopg.connect(autocommit=True) as conn:
        _insert_audit(conn, status, **fields)


def _insert_audit(conn: psycopg.Connection, status: str, **fields) -> None:
    cols = ["status", *fields]
    conn.execute(
        sql.SQL("insert into remediation.audit_log ({}) values ({})").format(
            _ids(cols), sql.SQL(", ").join(sql.Placeholder() * len(cols))
        ),
        [status, *fields.values()],
    )


def apply(spec: Spec, out_root: str | Path = "runs", dry_run: bool = False, log=print) -> dict:
    started = datetime.now(UTC)
    run_dir = Path(out_root) / f"{spec.name}-{started:%Y%m%dT%H%M%S%fZ}{'-dry-run' * dry_run}"
    run_dir.mkdir(parents=True)
    with psycopg.connect(autocommit=True) as setup:
        setup.execute(AUDIT_DDL)

    manifest: dict = {"fix": spec.name, "dry_run": dry_run, "spec_sha256": spec.sha256}
    t0 = time.perf_counter()
    conn = psycopg.connect()
    try:
        cur = conn.cursor()
        for setting in ("lock_timeout", "statement_timeout"):
            cur.execute(
                sql.SQL("set local {} = {}").format(
                    sql.Identifier(setting), sql.Literal(getattr(spec, setting))
                )
            )
        t = describe(cur, spec)
        manifest["table"] = t.label
        log(f"{'DRY RUN: ' if dry_run else ''}{spec.name} on {t.label}")

        key_list = _ids(k for k, _ in t.keys)
        cur.execute(
            sql.SQL(
                "create temp table fix_scope on commit drop as select {} from {} limit 0"
            ).format(key_list, t.name)
        )
        cur.execute(
            sql.SQL(
                "with locked as (select {} from {} where {} for update) "
                "insert into fix_scope select * from locked"
            ).format(key_list, t.name, sql.SQL(spec.where))
        )
        locked = manifest["rows_locked"] = cur.rowcount
        cur.execute("analyze fix_scope")
        log(f"locked {locked:,} rows (spec expects {spec.expect_rows:,})")
        if locked != spec.expect_rows:
            raise FixAborted(f"scope has {locked:,} rows, spec expects {spec.expect_rows:,}")

        manifest["backup_sha256"] = _write(run_dir / "backup.csv", _backup_csv(conn, t))
        manifest["rollback_sha256"] = _write(
            run_dir / "rollback.sql", _rollback_sql(conn, spec, t, locked)
        )
        log(f"backup and rollback script written to {run_dir}")

        cur.execute(
            sql.SQL("update {} t set {} from fix_scope s where {}").format(
                t.name,
                sql.SQL(", ").join(
                    sql.SQL("{} = {}").format(sql.Identifier(c), sql.SQL(spec.set[c]))
                    for c, _ in t.cols
                ),
                _key_join("t", "s", t.keys),
            )
        )
        changed = manifest["rows_changed"] = cur.rowcount
        if changed != locked:
            raise FixAborted(f"updated {changed:,} rows, locked {locked:,}")
        if spec.verify is not None:
            remaining = cur.execute(spec.verify).fetchone()[0]
            manifest["verify_result"] = remaining
            if remaining != 0:
                raise FixAborted(f"verify query returned {remaining}, expected 0")
        log(f"updated {changed:,} rows; all gates passed")

        manifest["seconds"] = round(time.perf_counter() - t0, 2)
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        audit = {
            "fix_name": spec.name,
            "started_at": started,
            "table_name": t.label,
            "rows_locked": locked,
            "rows_changed": changed,
            "run_dir": str(run_dir),
            "spec_sha256": spec.sha256,
            "backup_sha256": manifest["backup_sha256"],
            "rollback_sha256": manifest["rollback_sha256"],
        }
        if dry_run:
            conn.rollback()
            _audit("dry_run", **audit)
            log("dry run: rolled back, nothing was changed")
        else:
            _insert_audit(conn, "applied", **audit)
            conn.commit()
            log(f"committed in {manifest['seconds']}s")
        manifest["run_dir"] = str(run_dir)
        return manifest
    except Exception as exc:
        conn.rollback()
        _audit(
            "aborted",
            fix_name=spec.name,
            started_at=started,
            table_name=manifest.get("table"),
            rows_locked=manifest.get("rows_locked"),
            run_dir=str(run_dir),
            spec_sha256=spec.sha256,
            message=str(exc),
        )
        if isinstance(exc, FixAborted):
            raise
        raise FixAborted(str(exc)) from exc
    finally:
        conn.close()


def rollback(run_dir: str | Path, allow_skips: bool = False, log=print) -> int:
    run_dir = Path(run_dir)
    manifest = json.loads((run_dir / "manifest.json").read_text())
    if manifest["dry_run"]:
        raise FixAborted("this was a dry run; there is nothing to roll back")
    script = (run_dir / "rollback.sql").read_bytes()
    if hashlib.sha256(script).hexdigest() != manifest["rollback_sha256"]:
        raise FixAborted("rollback.sql does not match the hash recorded when the fix ran")

    started = datetime.now(UTC)
    notices: list[str] = []
    with psycopg.connect(autocommit=True) as conn:
        conn.add_notice_handler(lambda d: notices.append(d.message_primary or ""))
        if allow_skips:
            conn.execute("set safe_fix.allow_skips = on")
        fields = {
            "fix_name": manifest["fix"],
            "started_at": started,
            "table_name": manifest["table"],
            "rows_locked": manifest["rows_changed"],
            "run_dir": str(run_dir),
            "rollback_sha256": manifest["rollback_sha256"],
        }
        try:
            conn.execute(script.decode())
        except psycopg.Error as exc:
            conn.execute("rollback")
            _insert_audit(conn, "rollback_aborted", **fields, message=str(exc).strip())
            raise FixAborted(str(exc).strip()) from exc
        found = [n for n in notices if n.startswith("safe-fix rollback")]
        restored = int(found[-1].split()[3]) if found else 0
        _insert_audit(conn, "rolled_back", **fields, rows_changed=restored, message=found[-1])
        log(found[-1])
        return restored


def checksum(table: str) -> str:
    """md5 over every row in key order: equal before a fix and after its rollback."""
    with psycopg.connect() as conn:
        cur = conn.cursor()
        t = describe(cur, Spec("checksum", table, {}, "true", 1))
        query = sql.SQL("select md5(string_agg(t::text, E'\\n' order by {})) from {} t").format(
            _ids((k for k, _ in t.keys), "t."), t.name
        )
        return cur.execute(query).fetchone()[0]


def main() -> None:
    from safe_fix import demo

    parser = argparse.ArgumentParser(prog="safe-fix", description=__doc__.split("\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("apply", help="run a fix (or --dry-run it)")
    p.add_argument("spec")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--out", default="runs")
    p = sub.add_parser("rollback", help="undo a fix from its run directory")
    p.add_argument("run_dir")
    p.add_argument("--allow-skips", action="store_true")
    p = sub.add_parser("checksum", help="md5 of a table's rows in key order")
    p.add_argument("table")
    p = sub.add_parser("demo", help="create the example table public.shipments")
    p.add_argument("--rows", type=int, default=2_000_000)
    args = parser.parse_args()

    try:
        if args.command == "apply":
            apply(load_spec(args.spec), args.out, args.dry_run)
        elif args.command == "rollback":
            rollback(args.run_dir, args.allow_skips)
        elif args.command == "checksum":
            print(checksum(args.table))
        else:
            demo.create(args.rows)
    except (FixAborted, ValueError) as exc:
        print(f"aborted: {exc}", file=sys.stderr)
        sys.exit(1)
