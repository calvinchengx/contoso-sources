"""Invariants for the source systems.

Static checks on the specs and scripts — no Docker, no network.

THESE MOVED HERE FROM A PLATFORM, with the vendors themselves. They were
written in `fabric-platform-notebook-pipelines`, which carried its own copy of
every spec and serve.js; that copy was byte-identical to this repo's, so the
two agreed by accident of history rather than by structure. A test that asserts
what a vendor does belongs with the vendor, or it only protects the one
consumer that happens to hold a duplicate.
"""

import pathlib
import re
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
# The vendors are this repository's top level -- contoso-pos/, contoso-web/,
# contoso-reference/ -- rather than a `sources/` subdirectory inside a consumer.
SOURCES = ROOT
SPECS = sorted(SOURCES.glob("*/openapi.yaml"))

sys.path.insert(0, str(ROOT / "scripts"))
from materialise_sources import paginate  # noqa: E402


def test_there_is_at_least_one_source():
    assert SPECS, "no source specs found"


def test_every_spec_has_a_serve_script():
    """A spec without a script is a spec mokapi GENERATES bodies for.

    Measured against mokapi v0.50.0: schema generation is random per request and
    random in shape — optional properties are dropped per row — so a generated
    body cannot back an exact-count assertion. Every source must serve bytes
    from the seeded generators instead.
    """
    missing = [s.parent.name for s in SPECS if not (s.parent / "serve.js").exists()]
    assert not missing, f"these sources would serve generated data: {missing}"


def test_serve_scripts_read_files_rather_than_inventing_bodies():
    for spec in SPECS:
        js = (spec.parent / "serve.js").read_text(encoding="utf-8")
        assert "mokapi/file" in js, f"{spec.parent.name}: serves no file"
        assert "faker" not in js, f"{spec.parent.name}: fabricates data"


def test_every_operation_requires_a_key():
    """The extract steps assert that a wrong key is refused. That assertion is
    only meaningful if the API actually demands one."""
    for spec in SPECS:
        text = spec.read_text(encoding="utf-8")
        assert "securitySchemes" in text, f"{spec.parent.name}: no auth declared"
        ops = len(re.findall(r"^\s{6}operationId:", text, re.M))
        secured = len(re.findall(r"^\s{6}security:", text, re.M))
        assert ops == secured, (
            f"{spec.parent.name}: {ops} operations, {secured} declare security"
        )


def test_pages_reassemble_into_exactly_the_original_bytes():
    """Paging that loses or duplicates a row is worse than not paging.

    The whole export must be recoverable from its parts, byte for byte, or
    every count downstream is measuring a different dataset than the vendor
    sent — and would still look self-consistent while doing it.
    """
    body = b"".join(b'{"id":%d}\n' % i for i in range(50_000))
    pages = paginate(body, keep_header=False, page_bytes=64_000)
    assert len(pages) > 1, "the sample did not split, so nothing was tested"
    assert b"".join(pages) == body


def test_every_csv_page_repeats_the_header():
    """Each part has to be independently readable.

    Spark reads the landed directory with `header=True`. A part missing the
    header turns its first record into column names — silently, since the
    result is still a dataframe.
    """
    header = b"customer_id,name,country\n"
    rows = [b"c%d,Name %d,US\n" % (i, i) for i in range(50_000)]
    pages = paginate(header + b"".join(rows), keep_header=True, page_bytes=64_000)
    assert len(pages) > 1, "the sample did not split, so nothing was tested"
    for i, page in enumerate(pages):
        assert page.startswith(header), f"page {i + 1} has no header row"
    rebuilt = header + b"".join(p[len(header) :] for p in pages)
    assert rebuilt == header + b"".join(rows)


def test_paging_never_splits_a_record():
    sample = b"".join(b'{"id":%d}\n' % i for i in range(50_000))
    for page in paginate(sample, False, page_bytes=64_000):
        assert page.endswith(b"\n"), "a page ends mid-record"
        for line in page.splitlines():
            assert line.startswith(b'{"id":') and line.endswith(b"}"), line[:40]


def test_paged_operations_declare_their_paging():
    """The spec is what OpenMetadata ingests and what a client reads.

    An endpoint that pages without saying so leaves every caller to discover it
    by getting a partial answer that looks complete.

    WHETHER A VENDOR PAGES is decided by the `page` PARAMETER, not by the word
    appearing somewhere in the file. Contoso Reference does not page and says so
    in prose — it serves a Parquet file that cannot be split on line boundaries
    — and matching on the bare word marked it as a paging vendor missing its
    headers. The check runs both ways so neither half can be declared alone.
    """
    for spec in SPECS:
        text = spec.read_text(encoding="utf-8")
        declares_param = bool(re.search(r"^\s+name: page$", text, re.M))
        headers = [f for f in ("X-Total-Pages", "X-Page") if f in text]
        if not declares_param and not headers:
            continue
        assert declares_param, (
            f"{spec.parent.name}: advertises {headers} but declares no `page` "
            f"parameter, so a caller cannot ask for the rest"
        )
        for field in ("X-Total-Pages", "X-Page"):
            assert field in text, f"{spec.parent.name}: pages but no {field} header"


def test_serve_scripts_do_not_hardcode_a_page_count():
    """The page count belongs to the data, not to the handler.

    `make sources` decides how many pages there are. A number in the script is
    a second source of truth that goes stale the moment the page size moves,
    and the API would then advertise a count the directory cannot serve.
    """
    for spec in SPECS:
        js = spec.parent / "serve.js"
        if "X-Total-Pages" not in js.read_text(encoding="utf-8"):
            continue
        assert "pages.txt" in js.read_text(encoding="utf-8"), (
            f"{spec.parent.name}: page count is not read from the data"
        )


def test_specs_are_pinned_to_no_host_we_do_not_control():
    for spec in SPECS:
        for url in re.findall(
            r"^\s*- url:\s*(\S+)", spec.read_text(encoding="utf-8"), re.M
        ):
            assert "localhost" in url, f"{spec.parent.name}: points at {url}"


def test_the_reference_vendor_serves_binary_through_the_only_path_that_survives():
    """Parquet must go out on `response.data` as raw bytes, never `response.body`.

    THE FAILURE THIS PREVENTS IS SILENT. mokapi's text path takes a Go string
    into goja, which decodes it as UTF-8 and replaces every invalid sequence —
    so binary comes back mangled with a 200 attached. Measured against these
    exact files: fx_rates.parquet goes 2,268 bytes -> 3,301, a 46% inflation,
    with the `PAR1` magic AND the `PAR1` footer both still in place. Nothing
    downstream of a boundary check would notice.

    Only `response.data` holding a byte slice is passed through unmarshalled
    (providers/openapi/handler.go), and only `open(path, {as: 'binary'})`
    produces one — `read()` from 'mokapi/file' is the lossy path.
    """
    serve = (SOURCES / "contoso-reference" / "serve.js").read_text(encoding="utf-8")
    assert "{ as: 'binary' }" in serve or '{as: "binary"}' in serve, (
        "contoso-reference must open its Parquet with {as: 'binary'}; "
        "read() decodes bytes as UTF-8 and corrupts them"
    )
    # The parquet must not be handed to `response.body` under any spelling.
    body_assignments = re.findall(r"response\.body\s*=\s*(.+)", serve)
    assert not body_assignments, (
        f"contoso-reference assigns response.body ({body_assignments}) — that "
        f"path is a Go string and cannot carry Parquet without corrupting it"
    )


@pytest.mark.fixtures
def test_the_reference_spec_documents_the_columns_the_vendor_actually_sends():
    """The spec's schemas must match the Parquet the generator produces.

    These bodies are binary, so the schemas document COLUMNS rather than being
    marshalled into a response — which makes them the sort of documentation
    that rots unwatched. OpenMetadata surfaces them as the vendor's schema, and
    gold's rollup is written against these names, so a generator that renamed a
    column would leave the spec describing a table nobody serves.
    """
    import yaml

    spec = yaml.safe_load(
        (SOURCES / "contoso-reference" / "openapi.yaml").read_text(encoding="utf-8")
    )
    schemas = spec["components"]["schemas"]

    import reference_data as ref

    fx, hierarchy, _ = ref._built()
    for component, rows in (("FxRate", fx), ("ProductHierarchy", hierarchy)):
        published = set(schemas[component]["properties"])
        actual = set(rows[0])
        assert published == actual, (
            f"{component}: the spec documents {sorted(published)} but the "
            f"vendor sends {sorted(actual)}"
        )


@pytest.mark.fixtures
def test_reference_data_is_small_enough_to_serve_whole():
    """This vendor does not page, and Parquet cannot be paged.

    The line splitter would not refuse a Parquet file — it would return it
    intact today, because joining split lines reconstructs the bytes, and start
    corrupting it the day the export crosses PAGE_BYTES. A binary format with a
    footer has no line boundaries to split on. So the assumption that it fits
    in one response is checked here rather than left to hold by luck.
    """
    import reference_data as ref
    from materialise_sources import PAGE_BYTES

    for name, blob in ref.export(ref.API_KEY).items():
        assert blob[:4] == b"PAR1" and blob[-4:] == b"PAR1", name
        assert len(blob) <= PAGE_BYTES, (
            f"{name} is {len(blob):,} bytes, past the {PAGE_BYTES:,} served "
            f"whole — Parquet cannot be paged, so this needs a real answer"
        )
