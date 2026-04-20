"""Generate database review documentation snapshots from a live SQLite DB.

Author: OpenAI Codex
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


TRUNCATE_CHARS = 400
BUSY_TIMEOUT_MS = 30_000
APPENDIX_SAMPLE_TABLES = frozenset({"fundamentals_raw"})


@dataclass(frozen=True)
class TableStats:
    """Live table row count and on-disk size from SQLite."""

    row_count: int
    size_bytes: int


@dataclass(frozen=True)
class TableInventoryEntry:
    """Narrative metadata for one documented table."""

    table_name: str
    review_focus: str
    logical_refs: str


TABLE_GROUPS: tuple[tuple[str, tuple[TableInventoryEntry, ...]], ...] = (
    (
        "Identity And Catalog",
        (
            TableInventoryEntry(
                table_name="providers",
                logical_refs="referenced logically by provider-scoped tables",
                review_focus="keep the registry narrow and avoid leaking runtime config into it",
            ),
            TableInventoryEntry(
                table_name="exchange",
                logical_refs="referenced physically by `exchange_provider.exchange_id`",
                review_focus="keep the canonical exchange table narrow while it coexists with `canonical_exchange_code` elsewhere",
            ),
            TableInventoryEntry(
                table_name="exchange_provider",
                logical_refs="maps provider exchange codes to canonical exchange identity",
                review_focus="check whether provider-owned exchange metadata belongs here and whether exchange-slice rewrites stay cheap",
            ),
            TableInventoryEntry(
                table_name="securities",
                logical_refs="referenced logically by most downstream tables",
                review_focus="check whether display metadata belongs here or in a separate cache",
            ),
            TableInventoryEntry(
                table_name="supported_tickers",
                logical_refs="links provider catalog rows to `security_id`",
                review_focus="highest-priority catalog table; review duplicate metadata and scope indexes",
            ),
        ),
    ),
    (
        "Raw Ingestion And State",
        (
            TableInventoryEntry(
                table_name="fundamentals_raw",
                logical_refs="provider symbol in `supported_tickers`, `security_id` in `securities`",
                review_focus="wide-row storage, JSON payload size, and latest-row-only semantics",
            ),
            TableInventoryEntry(
                table_name="fundamentals_fetch_state",
                logical_refs="provider symbol in `supported_tickers`",
                review_focus="retry/backoff query shape vs index set",
            ),
            TableInventoryEntry(
                table_name="security_listing_status",
                logical_refs="`security_id` in `securities`",
                review_focus="primary-listing filter cost and purge trigger responsibilities",
            ),
            TableInventoryEntry(
                table_name="fundamentals_normalization_state",
                logical_refs="provider symbol in `supported_tickers`, `security_id` in `securities`",
                review_focus="whether this watermark table is minimal and sufficient",
            ),
            TableInventoryEntry(
                table_name="market_data_fetch_state",
                logical_refs="provider symbol in `supported_tickers`",
                review_focus="same pattern as fundamentals state; check duplication vs simplicity",
            ),
        ),
    ),
    (
        "Canonical Analytics",
        (
            TableInventoryEntry(
                table_name="financial_facts",
                logical_refs="`security_id` in `securities`",
                review_focus="hottest fact table; check row width, nullable PK parts, and latest-fact indexes",
            ),
            TableInventoryEntry(
                table_name="financial_facts_refresh_state",
                logical_refs="`security_id` in `securities`",
                review_focus="verify it still adds value beyond `fundamentals_normalization_state`",
            ),
            TableInventoryEntry(
                table_name="market_data",
                logical_refs="`security_id` in `securities`",
                review_focus="latest-snapshot access and time-series retention",
            ),
            TableInventoryEntry(
                table_name="metrics",
                logical_refs="`security_id` in `securities`",
                review_focus="screen-read performance and lack of historical versions",
            ),
            TableInventoryEntry(
                table_name="metric_compute_status",
                logical_refs="`security_id` in `securities`",
                review_focus="failure-report read shape and duplication with `metrics` freshness",
            ),
        ),
    ),
    (
        "FX",
        (
            TableInventoryEntry(
                table_name="fx_supported_pairs",
                logical_refs="canonical pair used by `fx_refresh_state`",
                review_focus="alias vs canonical pair modeling",
            ),
            TableInventoryEntry(
                table_name="fx_refresh_state",
                logical_refs="logical ref to canonical pairs in provider catalog",
                review_focus="whether coverage state justifies a dedicated table",
            ),
            TableInventoryEntry(
                table_name="fx_rates",
                logical_refs="no enforced FK",
                review_focus="largest FX table; pair/date access path and `rate_text` storage choice",
            ),
        ),
    ),
    (
        "Housekeeping",
        (
            TableInventoryEntry(
                table_name="schema_migrations",
                logical_refs="none",
                review_focus="low priority; check whether single-row semantics are guaranteed",
            ),
        ),
    ),
)


TABLE_SEQUENCE = tuple(
    entry.table_name for _, entries in TABLE_GROUPS for entry in entries
)


def _quote_ident(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _format_human_bytes(size_bytes: int) -> str:
    gib = size_bytes / (1024**3)
    mib = size_bytes / (1024**2)
    kib = size_bytes / 1024
    if gib >= 1:
        return f"{gib:.2f} GiB"
    if mib >= 1:
        return f"{mib:.1f} MiB"
    return f"{kib:.1f} KiB"


def _format_int(value: int) -> str:
    return f"{value:,}"


def _truncate_string(value: str, limit: int = TRUNCATE_CHARS) -> str:
    if len(value) <= limit:
        return value
    byte_length = len(value.encode("utf-8"))
    return f"{value[:limit]}... <truncated; {byte_length} bytes total>"


def _normalized_sample_value(value: Any, truncate_chars: int = TRUNCATE_CHARS) -> Any:
    if isinstance(value, str):
        return _truncate_string(value, limit=truncate_chars)
    return value


def _snapshot_date() -> str:
    return date.today().isoformat()


def _database_uri(database_path: Path) -> str:
    return f"{database_path.resolve().as_uri()}?mode=ro"


def connect_database_readonly(database_path: Path) -> sqlite3.Connection:
    """Open the SQLite database in read-only mode for doc generation."""

    conn = sqlite3.connect(
        _database_uri(database_path),
        uri=True,
        timeout=BUSY_TIMEOUT_MS / 1000,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_MS}")
    return conn


def _load_table_stats(conn: sqlite3.Connection, table_name: str) -> TableStats:
    """Load stats for one table via the aggregated dbstat helper."""

    return load_all_table_stats(conn, [table_name])[table_name]


def load_all_table_stats(
    conn: sqlite3.Connection,
    table_names: Sequence[str],
) -> dict[str, TableStats]:
    """Load dbstat-backed row counts and table sizes for many tables at once."""

    if not table_names:
        return {}
    try:
        placeholders = ", ".join("?" for _ in table_names)
        rows = conn.execute(
            """
            SELECT
                name,
                COALESCE(SUM(CASE WHEN pagetype = 'leaf' THEN ncell ELSE 0 END), 0)
                    AS row_count,
                COALESCE(SUM(pgsize), 0) AS size_bytes
            FROM dbstat
            WHERE name IN ("""
            + placeholders
            + """)
            GROUP BY name
            """,
            list(table_names),
        ).fetchall()
    except sqlite3.OperationalError as exc:
        raise RuntimeError(
            "SQLite dbstat is required to generate database review docs."
        ) from exc
    stats = {
        str(row["name"]): TableStats(
            row_count=int(row["row_count"] or 0),
            size_bytes=int(row["size_bytes"] or 0),
        )
        for row in rows
    }
    return {
        table_name: stats.get(table_name, TableStats(row_count=0, size_bytes=0))
        for table_name in table_names
    }


def fetch_sample_rows(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    limit: int = 5,
    truncate_chars: int = TRUNCATE_CHARS,
) -> list[dict[str, Any]]:
    """Return a cheap first-N sample window without explicit ordering."""

    rows = conn.execute(
        f"""
        SELECT *
        FROM {_quote_ident(table_name)}
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        {
            key: _normalized_sample_value(row[key], truncate_chars=truncate_chars)
            for key in row.keys()
        }
        for row in rows
    ]


def render_live_stats_block(
    table_name: str,
    stats: TableStats,
    *,
    snapshot_date: str,
) -> str:
    """Render the generated live-stats section body for one table page."""

    avg_bytes = 0.0 if stats.row_count == 0 else stats.size_bytes / stats.row_count
    return "\n".join(
        [
            f"- Snapshot source: `data/pyvalue.db` on `{snapshot_date}`",
            f"- Row count: `{_format_int(stats.row_count)}`",
            f"- Table size: `{_format_int(stats.size_bytes)} bytes` (`{_format_human_bytes(stats.size_bytes)}`)",
            f"- Approximate bytes per row: `{avg_bytes:,.1f}`",
        ]
    )


def render_sample_rows_block(
    table_name: str,
    rows: Sequence[dict[str, Any]],
    *,
    snapshot_date: str,
) -> str:
    """Render the generated sample-row section body for one table page."""

    sample_json = json.dumps(list(rows), indent=2, ensure_ascii=True)
    return "\n".join(
        [
            f"- Snapshot source: `data/pyvalue.db` on `{snapshot_date}`",
            (
                f"- Sample window: first `{len(rows)}` rows returned by SQLite "
                "using `LIMIT` with no `ORDER BY`"
            ),
            "",
            "```json",
            sample_json,
            "```",
        ]
    )


def render_wide_table_inline_sample_block(table_name: str) -> str:
    """Render the inline note for wide-table samples stored in the appendix."""

    return (
        "Wide-table sample rows live in the "
        f"[Sample Rows appendix](../sample-rows.md#{table_name})."
    )


def render_sample_rows_appendix(
    appendix_tables: Iterable[str],
    samples_by_table: Mapping[str, Sequence[dict[str, Any]]],
    *,
    snapshot_date: str,
) -> str:
    """Render the appendix body for tables whose samples stay out of line."""

    sections: list[str] = []
    for table_name in appendix_tables:
        sample_json = json.dumps(
            list(samples_by_table[table_name]), indent=2, ensure_ascii=True
        )
        sections.extend(
            [
                f"## `{table_name}`",
                "",
                f"- Snapshot source: `data/pyvalue.db` on `{snapshot_date}`",
                (
                    f"- Sample window: first `{len(samples_by_table[table_name])}` "
                    "rows returned by SQLite using `LIMIT` with no `ORDER BY`"
                ),
                "",
                "```json",
                sample_json,
                "```",
                "",
            ]
        )
    return "\n".join(sections).rstrip()


def _marker_block(marker_name: str, content: str) -> str:
    return (
        f"<!-- BEGIN {marker_name} -->\n{content.rstrip()}\n<!-- END {marker_name} -->"
    )


def _replace_marker_block(text: str, marker_name: str, content: str) -> str:
    pattern = re.compile(
        rf"<!-- BEGIN {re.escape(marker_name)} -->.*?<!-- END {re.escape(marker_name)} -->",
        re.DOTALL,
    )
    replacement = _marker_block(marker_name, content)
    if pattern.search(text):
        return pattern.sub(replacement, text, count=1)
    return text


def _upsert_section(
    text: str,
    *,
    heading: str,
    marker_name: str,
    content: str,
    insert_before_heading: str | None = None,
) -> str:
    section = f"## {heading}\n\n{_marker_block(marker_name, content)}"
    pattern = re.compile(
        rf"^## {re.escape(heading)}\n\n.*?(?=^## |\Z)",
        re.DOTALL | re.MULTILINE,
    )
    if pattern.search(text):
        return pattern.sub(section + "\n\n", text, count=1)
    insertion = section + "\n\n"
    if insert_before_heading is not None:
        before_pattern = re.compile(
            rf"^## {re.escape(insert_before_heading)}\n",
            re.MULTILINE,
        )
        match = before_pattern.search(text)
        if match is not None:
            return text[: match.start()] + insertion + text[match.start() :]
    if not text.endswith("\n"):
        text += "\n"
    return text + "\n" + insertion


def sync_table_live_stats(
    path: Path,
    *,
    table_name: str,
    stats: TableStats,
    snapshot_date: str,
) -> None:
    """Update one table page with the generated live-stats block."""

    text = path.read_text(encoding="utf-8")
    text = _upsert_section(
        text,
        heading="Live Stats",
        marker_name="generated_live_stats",
        content=render_live_stats_block(
            table_name,
            stats,
            snapshot_date=snapshot_date,
        ),
    )
    path.write_text(text, encoding="utf-8")


def sync_table_sample_rows(
    path: Path,
    *,
    table_name: str,
    sample_rows: Sequence[dict[str, Any]],
    snapshot_date: str,
) -> None:
    """Update one table page with the generated sample-row block."""

    text = path.read_text(encoding="utf-8")
    sample_content = (
        render_wide_table_inline_sample_block(table_name)
        if table_name in APPENDIX_SAMPLE_TABLES
        else render_sample_rows_block(
            table_name,
            sample_rows,
            snapshot_date=snapshot_date,
        )
    )
    text = _upsert_section(
        text,
        heading="Sample Rows",
        marker_name="generated_sample_rows",
        content=sample_content,
        insert_before_heading="Review Notes",
    )
    path.write_text(text, encoding="utf-8")


def sync_table_doc_page(
    path: Path,
    *,
    table_name: str,
    stats: TableStats,
    sample_rows: Sequence[dict[str, Any]],
    snapshot_date: str,
) -> None:
    """Update one table page with generated live-stats and sample-row blocks."""

    sync_table_live_stats(
        path,
        table_name=table_name,
        stats=stats,
        snapshot_date=snapshot_date,
    )
    sync_table_sample_rows(
        path,
        table_name=table_name,
        sample_rows=sample_rows,
        snapshot_date=snapshot_date,
    )


def render_table_inventory_block(
    *,
    snapshot_date: str,
    stats_by_table: dict[str, TableStats],
) -> str:
    """Render the generated inventory tables grouped by schema area."""

    lines = [
        f"All row counts and table sizes below come from the live `data/pyvalue.db` snapshot on `{snapshot_date}`. Sizes refer to the table object's own pages, not the size of its secondary indexes.",
        "",
    ]
    for group_name, entries in TABLE_GROUPS:
        lines.extend(
            [
                f"## {group_name}",
                "",
                "| Table | Rows | Table size | Primary key | Main logical refs | Initial review focus |",
                "| --- | --- | --- | --- | --- | --- |",
            ]
        )
        for entry in entries:
            stats = stats_by_table[entry.table_name]
            pk_columns = ", ".join(
                f"`{column}`" for column in _primary_key_display(entry.table_name)
            )
            lines.append(
                "| "
                f"[{entry.table_name}](tables/{entry.table_name}.md) | "
                f"`{_format_int(stats.row_count)}` | "
                f"`{_format_human_bytes(stats.size_bytes)}` | "
                f"{pk_columns} | "
                f"{entry.logical_refs} | "
                f"{entry.review_focus} |"
            )
        lines.append("")
    return "\n".join(lines).rstrip()


def _primary_key_display(table_name: str) -> Sequence[str]:
    mapping: dict[str, Sequence[str]] = {
        "providers": ("provider_code",),
        "exchange": ("exchange_id",),
        "exchange_provider": ("provider", "provider_exchange_code"),
        "securities": ("security_id",),
        "supported_tickers": ("provider", "provider_symbol"),
        "fundamentals_raw": ("provider", "provider_symbol"),
        "fundamentals_fetch_state": ("provider", "provider_symbol"),
        "security_listing_status": ("security_id",),
        "fundamentals_normalization_state": ("provider", "provider_symbol"),
        "market_data_fetch_state": ("provider", "provider_symbol"),
        "financial_facts": (
            "security_id",
            "concept",
            "fiscal_period",
            "end_date",
            "unit",
            "accn",
        ),
        "financial_facts_refresh_state": ("security_id",),
        "market_data": ("security_id", "as_of"),
        "metrics": ("security_id", "metric_id"),
        "metric_compute_status": ("security_id", "metric_id"),
        "fx_supported_pairs": ("provider", "symbol"),
        "fx_refresh_state": ("provider", "canonical_symbol"),
        "fx_rates": ("provider", "rate_date", "base_currency", "quote_currency"),
        "schema_migrations": ("none; append-only version rows",),
    }
    return mapping[table_name]


def sync_table_inventory_page(
    path: Path,
    *,
    snapshot_date: str,
    stats_by_table: dict[str, TableStats],
) -> None:
    """Update the table inventory page with generated live row counts and sizes."""

    text = path.read_text(encoding="utf-8")
    generated = render_table_inventory_block(
        snapshot_date=snapshot_date,
        stats_by_table=stats_by_table,
    )
    marker_name = "generated_table_inventory"
    if f"<!-- BEGIN {marker_name} -->" in text:
        text = _replace_marker_block(text, marker_name, generated)
    else:
        start = text.find("## Identity And Catalog")
        if start == -1:
            raise ValueError(f"Could not locate table inventory section in {path}")
        prefix = text[:start].rstrip()
        text = prefix + "\n\n" + _marker_block(marker_name, generated) + "\n"
    path.write_text(text, encoding="utf-8")


def sync_sample_rows_appendix(
    path: Path,
    *,
    snapshot_date: str,
    samples_by_table: Mapping[str, Sequence[dict[str, Any]]],
) -> None:
    """Update the appendix page for wide-table samples."""

    generated = render_sample_rows_appendix(
        APPENDIX_SAMPLE_TABLES,
        samples_by_table,
        snapshot_date=snapshot_date,
    )
    if path.exists():
        text = path.read_text(encoding="utf-8")
    else:
        text = (
            "# Sample Rows Appendix\n\n"
            "This page holds first-5-row samples for wide tables that would make the\n"
            "inline per-table docs difficult to read.\n\n"
            "<!-- BEGIN generated_sample_rows_appendix -->\n"
            "<!-- END generated_sample_rows_appendix -->\n"
        )
    marker_name = "generated_sample_rows_appendix"
    if f"<!-- BEGIN {marker_name} -->" in text:
        text = _replace_marker_block(text, marker_name, generated)
    else:
        if not text.endswith("\n"):
            text += "\n"
        text += "\n" + _marker_block(marker_name, generated) + "\n"
    path.write_text(text, encoding="utf-8")


def generate_database_review_docs(
    *,
    database_path: Path,
    docs_root: Path,
    sample_rows_only: bool = False,
) -> None:
    """Regenerate checked-in database review docs from one SQLite database."""

    conn = connect_database_readonly(database_path)
    try:
        snapshot_date = _snapshot_date()
        samples_by_table = {
            table_name: fetch_sample_rows(conn, table_name)
            for table_name in TABLE_SEQUENCE
        }
        stats_by_table = (
            {} if sample_rows_only else load_all_table_stats(conn, TABLE_SEQUENCE)
        )
    finally:
        conn.close()

    tables_dir = docs_root / "tables"
    for table_name in TABLE_SEQUENCE:
        path = tables_dir / f"{table_name}.md"
        if not path.exists():
            raise FileNotFoundError(f"Missing table doc page: {path}")
        if sample_rows_only:
            sync_table_sample_rows(
                path,
                table_name=table_name,
                sample_rows=samples_by_table[table_name],
                snapshot_date=snapshot_date,
            )
        else:
            sync_table_doc_page(
                path,
                table_name=table_name,
                stats=stats_by_table[table_name],
                sample_rows=samples_by_table[table_name],
                snapshot_date=snapshot_date,
            )
    if not sample_rows_only:
        sync_table_inventory_page(
            docs_root / "table-inventory.md",
            snapshot_date=snapshot_date,
            stats_by_table=stats_by_table,
        )
    sync_sample_rows_appendix(
        docs_root / "sample-rows.md",
        snapshot_date=snapshot_date,
        samples_by_table=samples_by_table,
    )


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the CLI parser for the doc generator."""

    parser = argparse.ArgumentParser(
        description="Regenerate checked-in database review docs from a SQLite DB."
    )
    parser.add_argument(
        "--database",
        default="data/pyvalue.db",
        help="SQLite database snapshot to read (default: data/pyvalue.db).",
    )
    parser.add_argument(
        "--docs-root",
        default="docs/architecture/database",
        help="Database review docs root to update (default: docs/architecture/database).",
    )
    parser.add_argument(
        "--sample-rows-only",
        action="store_true",
        help="Refresh only sample-row sections using cheap LIMIT queries.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for database review doc generation."""

    parser = build_arg_parser()
    args = parser.parse_args(argv)
    generate_database_review_docs(
        database_path=Path(args.database),
        docs_root=Path(args.docs_root),
        sample_rows_only=args.sample_rows_only,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
