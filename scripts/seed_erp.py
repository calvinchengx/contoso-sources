#!/usr/bin/env python3
"""Make the ERP vendor exist: seed its database, then replay its history as DML.

THE INVERSION IS THE POINT. The fixture ships a change LOG, and a change log is
what CDC *produces* — not what a source system holds. So this seeds the table
the ERP would actually have and applies the events as INSERT / UPDATE / DELETE
in capture order, leaving Debezium to observe the write-ahead log and produce
the stream. Landing a file that DESCRIBES a change feed would be a different
and much weaker claim.

This is the CDC vendor's equivalent of `make materialise` for the HTTP ones: it
is what makes the vendor real, so it belongs to the sources repo rather than to
any consumer. A platform runs it; it does not own it.

Idempotent IN THE TABLE ONLY, by TRUNCATE rather than append -- and that is
not the same as an idempotent VENDOR, which this docstring used to claim.

The topic is untouched. A second run replays the whole history onto a broker
that still holds the first, so the stream doubles while the table does not:
measured three times at exactly 2 x 93,571 events against 28,800 surviving
rows. Consumers that gate on `inserted - deleted == count(*)` catch it and say
so; consumers that state their counts as a minimum see the doubled stream as
success, which is the shape of green that means nothing.

Fixing it here means deleting the topic before replaying, which needs a Kafka
admin client -- and this repo declares no dependencies, relying on whichever
platform runs it to install what the seeder imports. That contract is the real
obstacle and it should be decided rather than worked around: see G21 in
contoso-data-product's plan. Until then a platform must recreate the broker
between runs, and every consumer of this stream needs the reconciliation guard.
"""
from __future__ import annotations

import io
import json
import os
import pathlib
import sys
import time
import urllib.error
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
SCHEMA = ROOT / "contoso-erp" / "schema.sql"
CONNECTOR = ROOT / "contoso-erp" / "debezium-connector.json"

# The tracked columns, in the order the INSERT binds them. Named here rather
# than derived from the export, because a column appearing in the fixture that
# the schema does not have should fail loudly at bind time.
COLUMNS = ["erp_customer_id", "phone", "legal_name", "account_tier", "segment",
           "credit_band", "account_status", "payment_terms_days", "country",
           "effective_date"]
SETS = [c for c in COLUMNS if c != "erp_customer_id"]

INSERT = (f"INSERT INTO erp.customer ({', '.join(COLUMNS)}) "
          f"VALUES ({', '.join(['%s'] * len(COLUMNS))}) "
          f"ON CONFLICT (erp_customer_id) DO NOTHING")
UPDATE = (f"UPDATE erp.customer SET {', '.join(f'{c} = %s' for c in SETS)} "
          f"WHERE erp_customer_id = %s")
DELETE = "DELETE FROM erp.customer WHERE erp_customer_id = %s"


def events():
    """The vendor's own account of what happened, through its PUBLIC api.

    `export()` rather than the generator's internals: a consumer has only the
    published contract, and a private helper could change under a patch release
    without warning.
    """
    import erp_system as erp
    import pyarrow.parquet as pq

    blob = erp.export(erp.API_KEY)["changes.parquet"]
    rows = pq.read_table(io.BytesIO(blob)).to_pylist()
    # CAPTURE ORDER, which is deliberately not business order -- the fixture
    # disagrees between the two on purpose, and sorting by anything else would
    # quietly repair a disagreement the pipeline is supposed to face.
    rows.sort(key=lambda r: r["capture_seq"])
    return rows


def register_connector(connect_url: str, timeout: int = 180) -> None:  # noqa: C901
    """Point Debezium at the table. BEFORE the replay, never after.

    Start the connector afterwards and the history is captured by a snapshot
    rather than as a change stream -- which would still produce rows, and might
    even match on count, while testing the wrong thing entirely.
    """
    # The vendor's connector file, with only its TOPOLOGY overridden. Where the
    # database listens is a deployment fact, not vendor behaviour -- the file
    # names `erp-postgres` because that is what it was called in the platform it
    # came from. Everything that describes the CAPTURE (plugin, slot, table
    # list, snapshot mode, converters) is left exactly as the vendor wrote it.
    cfg = json.loads(CONNECTOR.read_text(encoding="utf-8"))
    cfg["config"]["database.hostname"] = os.environ.get("ERP_DB_HOST", "contoso-erp-db")
    body = json.dumps(cfg).encode()
    deadline = time.time() + timeout
    while True:
        try:
            req = urllib.request.Request(f"{connect_url}/connectors", data=body,
                                         headers={"Content-Type": "application/json"},
                                         method="POST")
            with urllib.request.urlopen(req, timeout=30) as r:
                print(f"seed_erp: connector registered ({r.status})", flush=True)
                break
        except urllib.error.HTTPError as e:
            if e.code == 409:  # already there: this is a re-run
                print("seed_erp: connector already registered", flush=True)
                return
            raise
        except OSError as e:
            if time.time() > deadline:
                raise SystemExit(f"seed_erp: Debezium never answered at {connect_url}: {e}")
            time.sleep(3)

    # REGISTERED IS NOT CAPTURING, and the difference is the whole claim.
    # POST returns 201 the moment the connector is CREATED; Debezium starts its
    # task asynchronously afterwards. Replay against that gap and the DML lands
    # before capture begins -- so the connector, arriving late, snapshots a
    # FINISHED table instead. Measured: the topic held 28,800 messages, exactly
    # 30,000 inserts minus 1,200 deletes, which is the final state and not the
    # 93,571-event history.
    #
    # RUNNING alone is not enough either: a connector reports RUNNING while its
    # task has already failed, so both are checked.
    name = cfg["name"]
    while True:
        try:
            with urllib.request.urlopen(f"{connect_url}/connectors/{name}/status",
                                        timeout=30) as r:
                st = json.loads(r.read())
        except OSError:
            st = {}
        conn_state = st.get("connector", {}).get("state")
        tasks = [t.get("state") for t in st.get("tasks", [])]
        if conn_state == "RUNNING" and tasks and all(t == "RUNNING" for t in tasks):
            print(f"seed_erp: connector {conn_state}, tasks {tasks} -- capturing",
                  flush=True)
            return
        if "FAILED" in (conn_state, *tasks):
            raise SystemExit(f"seed_erp: connector failed: {json.dumps(st)[:500]}")
        if time.time() > deadline:
            raise SystemExit(f"seed_erp: connector never reached RUNNING: "
                             f"{json.dumps(st)[:500]}")
        time.sleep(2)


def main() -> int:
    import erp_system as erp
    import psycopg

    dsn = os.environ.get(
        "ERP_DSN",
        "host=contoso-erp-db port=5432 dbname=erp user=contoso password=contoso-erp-dev")
    connect_url = os.environ.get("ERP_CONNECT_URL", "http://contoso-erp-connect:8083")

    log = events()
    expected = getattr(erp, "EXPECTED_ERP_CHANGE_EVENTS", None)
    if expected is not None and len(log) != expected:
        raise SystemExit(f"seed_erp: export gave {len(log)} events, generator "
                         f"declares {expected}")
    print(f"seed_erp: {len(log)} change events from the vendor's export", flush=True)

    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA.read_text(encoding="utf-8"))
            cur.execute("TRUNCATE erp.customer")
        conn.commit()

    # Connector first, then the DML it must observe.
    register_connector(connect_url)

    ins = upd = dele = 0
    with psycopg.connect(dsn, autocommit=False) as conn:
        with conn.cursor() as cur:
            for e in log:
                if e["op"] == "I":
                    cur.execute(INSERT, tuple(e[c] for c in COLUMNS)); ins += 1
                elif e["op"] == "U":
                    cur.execute(UPDATE, (*(e[c] for c in SETS), e["erp_customer_id"])); upd += 1
                else:
                    cur.execute(DELETE, (e["erp_customer_id"],)); dele += 1
        conn.commit()

    print(f"seed_erp: replayed I={ins} U={upd} D={dele} as real DML", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
