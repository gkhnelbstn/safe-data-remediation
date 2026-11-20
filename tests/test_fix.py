"""Each test pins one claim from the README, against a real PostgreSQL."""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path

import pytest

from safe_fix import demo
from safe_fix.fix import FixAborted, Spec, apply, checksum, load_spec, rollback

TEST_DB = "safe_fix_test"
SCOPE = "carrier_id in (3, 5) and shipped_on >= date '2025-06-01' and weight_kg > 5000"


@pytest.fixture
def db(monkeypatch):
    """A fresh throwaway database with a 20,000-row demo table; yields a connect function."""
    psycopg = pytest.importorskip("psycopg")
    if not os.environ.get("PGHOST"):
        pytest.skip("PGHOST not set; skipping database tests")
    try:
        admin = psycopg.connect(dbname=os.environ.get("PGDATABASE", "postgres"), autocommit=True)
    except psycopg.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    with admin:
        admin.execute(f"drop database if exists {TEST_DB} with (force)")
        admin.execute(f"create database {TEST_DB}")
    monkeypatch.setenv("PGDATABASE", TEST_DB)
    demo.create(20_000)
    return lambda: psycopg.connect(autocommit=True)


@pytest.fixture
def spec(db) -> Spec:
    with db() as conn:
        n = conn.execute(f"select count(*) from public.shipments where {SCOPE}").fetchone()[0]
    assert n > 1_000
    return Spec(
        name="weights",
        table="public.shipments",
        where=SCOPE,
        set={"weight_kg": "round(weight_kg / 1000, 3)"},
        expect_rows=n,
        verify=f"select count(*) from public.shipments where {SCOPE}",
    )


def statuses(db) -> list[str]:
    with db() as conn:
        return [r[0] for r in conn.execute("select status from remediation.audit_log order by id")]


def test_dry_run_changes_nothing_but_writes_the_files(db, spec, tmp_path) -> None:
    before = checksum("public.shipments")
    result = apply(spec, tmp_path, dry_run=True, log=lambda _: None)
    assert checksum("public.shipments") == before
    assert result["rows_changed"] == spec.expect_rows
    run = Path(result["run_dir"])
    assert {p.name for p in run.iterdir()} == {"backup.csv", "rollback.sql", "manifest.json"}
    assert statuses(db) == ["dry_run"]
    with pytest.raises(FixAborted, match="dry run"):
        rollback(run, log=lambda _: None)


def test_apply_then_rollback_restores_the_table_exactly(db, spec, tmp_path) -> None:
    before = checksum("public.shipments")
    result = apply(spec, tmp_path, log=lambda _: None)
    run = Path(result["run_dir"])
    assert result["rows_changed"] == spec.expect_rows
    assert len((run / "backup.csv").read_text().splitlines()) == spec.expect_rows + 1
    after = checksum("public.shipments")
    assert after != before

    assert rollback(run, log=lambda _: None) == spec.expect_rows
    assert checksum("public.shipments") == before
    assert statuses(db) == ["applied", "rolled_back"]


def test_wrong_expected_count_aborts_with_nothing_changed(db, spec, tmp_path) -> None:
    before = checksum("public.shipments")
    with pytest.raises(FixAborted, match="spec expects"):
        apply(replace(spec, expect_rows=spec.expect_rows + 1), tmp_path, log=lambda _: None)
    assert checksum("public.shipments") == before
    assert statuses(db) == ["aborted"]


def test_failing_verify_query_aborts_with_nothing_changed(db, spec, tmp_path) -> None:
    before = checksum("public.shipments")
    with pytest.raises(FixAborted, match="verify"):
        apply(replace(spec, verify="select 1"), tmp_path, log=lambda _: None)
    assert checksum("public.shipments") == before
    assert statuses(db) == ["aborted"]


def test_rollback_refuses_when_a_row_changed_after_the_fix(db, spec, tmp_path) -> None:
    run = Path(apply(spec, tmp_path, log=lambda _: None)["run_dir"])
    with db() as conn:
        edited = conn.execute(
            "update public.shipments set weight_kg = 1 where shipment_id = ("
            "  select min(shipment_id) from public.shipments"
            "  where carrier_id in (3, 5) and shipped_on >= date '2025-06-01')"
        ).rowcount
    assert edited == 1
    edited_state = checksum("public.shipments")

    with pytest.raises(FixAborted, match="restored only"):
        rollback(run, log=lambda _: None)
    assert checksum("public.shipments") == edited_state  # all or nothing

    assert rollback(run, allow_skips=True, log=lambda _: None) == spec.expect_rows - 1
    assert statuses(db) == ["applied", "rollback_aborted", "rolled_back"]


def test_rollback_refuses_a_modified_script(db, spec, tmp_path) -> None:
    run = Path(apply(spec, tmp_path, log=lambda _: None)["run_dir"])
    with open(run / "rollback.sql", "a") as f:
        f.write("-- edited\n")
    with pytest.raises(FixAborted, match="does not match"):
        rollback(run, log=lambda _: None)


def test_awkward_values_round_trip(db, tmp_path) -> None:
    awkward = [None, "", "O'Brien", "back\\slash", "line\nbreak", "çğış 東京", "  spaced  "]
    with db() as conn:
        conn.execute("create table public.notes (id int primary key, note text, n int)")
        with conn.cursor() as cur:
            cur.executemany(
                "insert into public.notes values (%s, %s, %s)",
                [(i, v, i) for i, v in enumerate(awkward)],
            )
    before = checksum("public.notes")
    fix = Spec("notes", "public.notes", {"note": "upper(note)", "n": "null"}, "true", len(awkward))
    run = Path(apply(fix, tmp_path, log=lambda _: None)["run_dir"])
    assert checksum("public.notes") != before
    assert rollback(run, log=lambda _: None) == len(awkward)
    assert checksum("public.notes") == before


def test_refuses_a_table_without_primary_key(db, tmp_path) -> None:
    with db() as conn:
        conn.execute("create table public.loose (v int)")
        conn.execute("insert into public.loose values (1)")
    with pytest.raises(FixAborted, match="no primary key"):
        apply(Spec("loose", "public.loose", {"v": "2"}, "true", 1), tmp_path, log=lambda _: None)


def test_spec_needs_an_expected_count(tmp_path) -> None:
    path = tmp_path / "fix.toml"
    path.write_text('name = "x"\ntable = "t"\nwhere = "true"\nset = { a = "1" }\n')
    with pytest.raises(ValueError, match="expect_rows"):
        load_spec(path)


def test_example_spec_loads() -> None:
    spec = load_spec(Path(__file__).parents[1] / "examples" / "shipment-weights-in-grams.toml")
    assert spec.expect_rows == 293_147
