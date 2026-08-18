# contoso-sources

**The vendors, and nothing else.** Four source systems as they appear to anyone
downstream: three OpenAPI services and one database whose history arrives as a
change stream.

No platform, no product, no pipeline. This repo answers one question — *what
does Contoso's data look like coming out of the systems that produce it* — and
it answers it the same way for every consumer.

## Why its own repository

`fabric-platform-notebook-pipelines` and `contoso-data-product-fabric-airflow3` both need these
vendors, and until now each carried its own copy of the specs and its own
materialiser. **Two copies of a vendor is where a comparison dies.** The two
platforms are held to the same bronze row counts; that claim is only meaningful
if both pulled from the same bytes, and a copy drifts the moment one side
regenerates and the other does not.

## The four vendors

| vendor | kind | format | shape |
|---|---|---|---|
| `contoso_pos` | OpenAPI | delimited text + JSON Lines | **paged** — `X-Total-Pages`, parts served separately |
| `contoso_web` | OpenAPI | JSON arrays | **nested** — an order carries its own `lines` |
| `contoso_reference` | OpenAPI | **Parquet** | master data, not paged, ~4 KB |
| `contoso_erp` | Postgres + CDC | change events | history as a **stream**, not a snapshot |

Three are operational systems. Reference is the group data office's publisher —
a vendor rather than a table maintained downstream, because that is what it is
in the business: its own owner, its own credential, its own schedule.

## `sources.yaml` is the contract

A consumer reads it to learn which vendors exist, what each serves and on which
port, and which Airflow Connection (or equivalent) names it. **It is a
declaration, not a compose file** — how a vendor gets stood up is the
consumer's business, and in production none of it is stood up at all, because
the vendors are real and only their connections differ.

## The exports are generated, not committed

```sh
make materialise      # ~194 MB into _data/, from the pinned generator wheels
```

`_data/` is gitignored. The generators are the same seeded ones the
fabric-emulator examples assert against, pinned in `pyproject.toml`, so the
bytes are identical for every consumer on every run. mokapi serves these files
rather than generating bodies, because its own generation is random per request
and random in shape — which cannot back an exact-count assertion.

Apache-2.0.
