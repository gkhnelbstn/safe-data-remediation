"""The example: public.shipments, where two carriers' feeds sent weights in grams from June.

Deterministic SQL, so the same ``--rows`` always gives the same table and the same scope.
Real weights are 5 to 2,000 kg; the buggy rows are 1,000 times that. ``reference`` has NULLs
and values with quotes, so the backup and checksum see awkward text too.
"""

from __future__ import annotations

import psycopg

DEMO_SQL = """
drop table if exists public.shipments;
create table public.shipments (
    shipment_id  bigint        primary key,
    carrier_id   integer       not null,
    shipped_on   date          not null,
    weight_kg    numeric(12,3) not null,
    reference    text,
    status       text          not null
);
insert into public.shipments
select n,
       c.carrier_id,
       c.shipped_on,
       case when c.carrier_id in (3, 5) and c.shipped_on >= date '2025-06-01'
            then w.kg * 1000 else w.kg end,
       case when n % 50 = 0 then null
            when n % 97 = 0 then 'O''Brien & Sons, order ' || n
            else 'REF-' || n end,
       (array['delivered', 'in_transit', 'returned'])[1 + n % 3]
from generate_series(1::bigint, {rows}) n
cross join lateral (select 1 + (n % 8)::int as carrier_id,
                           date '2025-01-01' + (n * 365 / {rows})::int as shipped_on) c
cross join lateral (select round(5 + (n * 7919 % 199500) / 100.0, 3) as kg) w;
analyze public.shipments;
"""


def create(rows: int) -> None:
    if not 1_000 <= rows <= 20_000_000:
        raise ValueError("--rows must be between 1,000 and 20,000,000")
    with psycopg.connect(autocommit=True) as conn:
        conn.execute(DEMO_SQL.format(rows=int(rows)))
        scope = conn.execute(
            "select count(*) from public.shipments"
            " where carrier_id in (3, 5) and shipped_on >= date '2025-06-01' and weight_kg > 5000"
        ).fetchone()[0]
    print(f"public.shipments: {rows:,} rows, {scope:,} with weights in grams")
