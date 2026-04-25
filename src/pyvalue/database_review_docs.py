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


@dataclass(frozen=True)
class ForeignKeyMetadata:
    """One outgoing foreign-key relationship."""

    from_columns: tuple[str, ...]
    ref_table: str
    ref_columns: tuple[str, ...]


@dataclass(frozen=True)
class IncomingForeignKeyMetadata:
    """One incoming foreign-key reference from another table."""

    from_table: str
    from_columns: tuple[str, ...]
    target_columns: tuple[str, ...]


@dataclass(frozen=True)
class IndexMetadata:
    """One documented SQLite index."""

    name: str
    columns: tuple[str, ...]
    where_clause: str | None = None


@dataclass(frozen=True)
class TableSchema:
    """Schema metadata needed for documentation generation."""

    primary_key: tuple[str, ...]
    foreign_keys: tuple[ForeignKeyMetadata, ...]
    unique_constraints: tuple[tuple[str, ...], ...]
    secondary_indexes: tuple[IndexMetadata, ...]


TABLE_GROUPS: tuple[tuple[str, tuple[TableInventoryEntry, ...]], ...] = (
    (
        "Identity And Catalog",
        (
            TableInventoryEntry(
                table_name="provider",
                logical_refs="referenced physically by `provider_exchange` and `provider_listing`",
                review_focus="keep the registry narrow and avoid leaking runtime config into it",
            ),
            TableInventoryEntry(
                table_name="exchange",
                logical_refs="referenced physically by `provider_exchange.exchange_id` and `listing.exchange_id`",
                review_focus="keep the canonical exchange table narrow and indexed for provider-catalog resolution",
            ),
            TableInventoryEntry(
                table_name="provider_exchange",
                logical_refs="maps provider exchange codes to canonical exchange identity",
                review_focus="check whether provider-owned exchange metadata belongs here and whether exchange-slice rewrites stay cheap",
            ),
            TableInventoryEntry(
                table_name="issuer",
                logical_refs="referenced physically by `listing.issuer_id`",
                review_focus="separate issuer metadata from listing identity and keep updates cheap",
            ),
            TableInventoryEntry(
                table_name="listing",
                logical_refs="canonical root for facts, prices, metrics, and primary-listing status",
                review_focus="maintain fast lookup by `(exchange_id, symbol)` and keep canonical status semantics clear",
            ),
            TableInventoryEntry(
                table_name="provider_listing",
                logical_refs="links provider catalog rows to canonical `listing_id`",
                review_focus="highest-priority provider catalog table; review provider slice rewrites and lookup indexes",
            ),
        ),
    ),
    (
        "Raw Ingestion And State",
        (
            TableInventoryEntry(
                table_name="fundamentals_raw",
                logical_refs="`provider_listing_id` in `provider_listing`",
                review_focus="wide-row storage, JSON payload size, hash versioning, and latest-row-only semantics",
            ),
            TableInventoryEntry(
                table_name="fundamentals_fetch_state",
                logical_refs="`provider_listing_id` in `provider_listing`",
                review_focus="active retry/backoff rows only; success is derived from raw payloads",
            ),
            TableInventoryEntry(
                table_name="fundamentals_normalization_state",
                logical_refs="`provider_listing_id` in `provider_listing`",
                review_focus="payload-hash watermark minimality",
            ),
            TableInventoryEntry(
                table_name="market_data_fetch_state",
                logical_refs="`provider_listing_id` in `provider_listing`",
                review_focus="same pattern as fundamentals state; check duplication vs simplicity",
            ),
        ),
    ),
    (
        "Canonical Analytics",
        (
            TableInventoryEntry(
                table_name="financial_facts",
                logical_refs="`listing_id` in `listing`",
                review_focus="hottest fact table; check row width, nullable PK parts, and latest-fact indexes",
            ),
            TableInventoryEntry(
                table_name="financial_facts_refresh_state",
                logical_refs="`listing_id` in `listing`",
                review_focus="verify it still adds value beyond `fundamentals_normalization_state`",
            ),
            TableInventoryEntry(
                table_name="market_data",
                logical_refs="`listing_id` in `listing`",
                review_focus="latest-snapshot access and time-series retention",
            ),
            TableInventoryEntry(
                table_name="metrics",
                logical_refs="`listing_id` in `listing`",
                review_focus="screen-read performance and lack of historical versions",
            ),
            TableInventoryEntry(
                table_name="metric_compute_status",
                logical_refs="`listing_id` in `listing`",
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
TABLE_ENTRY_BY_NAME = {
    entry.table_name: entry for _, entries in TABLE_GROUPS for entry in entries
}


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


def _normalize_sql_whitespace(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip())


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


def _load_index_columns(
    conn: sqlite3.Connection,
    index_name: str,
    *,
    include_sort_order: bool,
) -> tuple[str, ...]:
    rows = conn.execute(f"PRAGMA index_xinfo({_quote_ident(index_name)})").fetchall()
    columns: list[str] = []
    ordered_rows = sorted(
        (
            row
            for row in rows
            if int(row["key"]) == 1 and int(row["cid"]) >= 0 and row["name"] is not None
        ),
        key=lambda row: int(row["seqno"]),
    )
    for row in ordered_rows:
        column_name = str(row["name"])
        if include_sort_order and int(row["desc"]):
            columns.append(f"{column_name} DESC")
        else:
            columns.append(column_name)
    return tuple(columns)


def _load_index_where_clause(
    conn: sqlite3.Connection,
    index_name: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT sql
        FROM sqlite_master
        WHERE type = 'index' AND name = ?
        """,
        (index_name,),
    ).fetchone()
    sql = None if row is None else row["sql"]
    if sql is None:
        return None
    match = re.search(r"\bWHERE\b(?P<predicate>.+)$", str(sql), re.DOTALL)
    if match is None:
        return None
    return _normalize_sql_whitespace(match.group("predicate"))


def load_table_schema(
    conn: sqlite3.Connection,
    table_name: str,
) -> TableSchema:
    """Load schema metadata for one documented table."""

    table_info = conn.execute(
        f"PRAGMA table_info({_quote_ident(table_name)})"
    ).fetchall()
    primary_key = tuple(
        str(row["name"])
        for row in sorted(
            (row for row in table_info if int(row["pk"]) > 0),
            key=lambda row: int(row["pk"]),
        )
    )

    fk_rows = conn.execute(
        f"PRAGMA foreign_key_list({_quote_ident(table_name)})"
    ).fetchall()
    grouped_fks: dict[int, list[sqlite3.Row]] = {}
    for row in fk_rows:
        grouped_fks.setdefault(int(row["id"]), []).append(row)
    foreign_keys = tuple(
        ForeignKeyMetadata(
            from_columns=tuple(str(row["from"]) for row in ordered_rows),
            ref_table=str(ordered_rows[0]["table"]),
            ref_columns=tuple(
                str(row["to"]) if row["to"] is not None else "rowid"
                for row in ordered_rows
            ),
        )
        for _, ordered_rows in sorted(
            (
                (
                    fk_id,
                    sorted(rows, key=lambda row: int(row["seq"])),
                )
                for fk_id, rows in grouped_fks.items()
            ),
            key=lambda item: item[0],
        )
    )

    index_rows = conn.execute(
        f"PRAGMA index_list({_quote_ident(table_name)})"
    ).fetchall()
    unique_constraints: list[tuple[str, ...]] = []
    secondary_indexes: list[IndexMetadata] = []
    for row in index_rows:
        index_name = str(row["name"])
        is_unique = bool(row["unique"])
        if is_unique:
            if str(row["origin"]) == "pk":
                continue
            unique_constraints.append(
                _load_index_columns(
                    conn,
                    index_name,
                    include_sort_order=False,
                )
            )
            continue
        secondary_indexes.append(
            IndexMetadata(
                name=index_name,
                columns=_load_index_columns(
                    conn,
                    index_name,
                    include_sort_order=True,
                ),
                where_clause=_load_index_where_clause(conn, index_name),
            )
        )
    return TableSchema(
        primary_key=primary_key,
        foreign_keys=foreign_keys,
        unique_constraints=tuple(unique_constraints),
        secondary_indexes=tuple(secondary_indexes),
    )


def load_all_table_schemas(
    conn: sqlite3.Connection,
    table_names: Sequence[str],
) -> dict[str, TableSchema]:
    """Load schema metadata for many tables."""

    return {
        table_name: load_table_schema(conn, table_name) for table_name in table_names
    }


def build_incoming_foreign_keys(
    schema_by_table: Mapping[str, TableSchema],
) -> dict[str, tuple[IncomingForeignKeyMetadata, ...]]:
    """Build reverse foreign-key references for each documented table."""

    incoming: dict[str, list[IncomingForeignKeyMetadata]] = {
        table_name: [] for table_name in schema_by_table
    }
    for table_name, schema in schema_by_table.items():
        for foreign_key in schema.foreign_keys:
            if foreign_key.ref_table not in incoming:
                continue
            incoming[foreign_key.ref_table].append(
                IncomingForeignKeyMetadata(
                    from_table=table_name,
                    from_columns=foreign_key.from_columns,
                    target_columns=foreign_key.ref_columns,
                )
            )
    return {
        table_name: tuple(
            sorted(
                refs,
                key=lambda ref: (ref.from_table, ref.from_columns, ref.target_columns),
            )
        )
        for table_name, refs in incoming.items()
    }


def _sample_order_display(
    table_name: str,
    schema: TableSchema,
) -> tuple[str, ...]:
    if table_name == "schema_migrations":
        return ("version ASC",)
    if schema.primary_key:
        return tuple(f"{column} ASC" for column in schema.primary_key)
    return ("rowid ASC",)


def _sample_order_sql(
    table_name: str,
    schema: TableSchema,
) -> tuple[str, ...]:
    if table_name == "schema_migrations":
        return ('"version" ASC',)
    if schema.primary_key:
        return tuple(f"{_quote_ident(column)} ASC" for column in schema.primary_key)
    return ("rowid ASC",)


def _normalized_sample_row(
    table_name: str,
    row: sqlite3.Row,
    *,
    truncate_chars: int,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key in row.keys():
        value = row[key]
        if (
            table_name == "fundamentals_raw"
            and key == "data"
            and isinstance(value, str)
        ):
            normalized[key] = "<omitted>"
            normalized["data_bytes"] = len(value.encode("utf-8"))
            continue
        normalized[key] = _normalized_sample_value(value, truncate_chars=truncate_chars)
    return normalized


def fetch_sample_rows(
    conn: sqlite3.Connection,
    table_name: str,
    *,
    table_schema: TableSchema,
    limit: int = 5,
    truncate_chars: int = TRUNCATE_CHARS,
) -> list[dict[str, Any]]:
    """Return the deterministic first-N sample window for one table."""

    order_by = ", ".join(_sample_order_sql(table_name, table_schema))
    rows = conn.execute(
        f"""
        SELECT *
        FROM {_quote_ident(table_name)}
        ORDER BY {order_by}
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [
        _normalized_sample_row(table_name, row, truncate_chars=truncate_chars)
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
    order_description: Sequence[str],
) -> str:
    """Render the generated sample-row section body for one table page."""

    sample_json = json.dumps(list(rows), indent=2, ensure_ascii=True)
    return "\n".join(
        [
            f"- Snapshot source: `data/pyvalue.db` on `{snapshot_date}`",
            (
                f"- Sample window: first `{len(rows)}` rows returned by SQLite "
                f"ordered by `{', '.join(order_description)}`"
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
    order_by_table: Mapping[str, Sequence[str]],
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
                    f"rows returned by SQLite ordered by "
                    f"`{', '.join(order_by_table[table_name])}`"
                ),
                "- Wide payload columns are omitted and replaced with payload size metadata.",
                "",
                "```json",
                sample_json,
                "```",
                "",
            ]
        )
    return "\n".join(sections).rstrip()


def _format_column_tuple(columns: Sequence[str]) -> str:
    formatted = ", ".join(f"`{column}`" for column in columns)
    if len(columns) == 1:
        return formatted
    return f"({formatted})"


def render_keys_and_relationships_block(
    *,
    table_name: str,
    table_schema: TableSchema,
    incoming_foreign_keys: Sequence[IncomingForeignKeyMetadata],
    logical_refs: str,
) -> str:
    """Render the schema-derived keys/relationships section for one table page."""

    lines = [
        (
            f"- Primary key: {_format_column_tuple(table_schema.primary_key)}"
            if table_schema.primary_key
            else "- Primary key: none"
        )
    ]
    if table_schema.foreign_keys:
        lines.append("- Physical foreign keys:")
        lines.extend(
            [
                f"  - {_format_column_tuple(foreign_key.from_columns)} -> "
                f"`{foreign_key.ref_table}`.{_format_column_tuple(foreign_key.ref_columns)}"
                for foreign_key in table_schema.foreign_keys
            ]
        )
    else:
        lines.append("- Physical foreign keys: none")
    if incoming_foreign_keys:
        lines.append("- Physical references from other tables:")
        lines.extend(
            [
                f"  - `{incoming_ref.from_table}`."
                f"{_format_column_tuple(incoming_ref.from_columns)} -> "
                f"{_format_column_tuple(incoming_ref.target_columns)}"
                for incoming_ref in incoming_foreign_keys
            ]
        )
    else:
        lines.append("- Physical references from other tables: none")
    if table_schema.unique_constraints:
        lines.append("- Unique constraints beyond the primary key:")
        lines.extend(
            [
                f"  - {_format_column_tuple(unique_columns)}"
                for unique_columns in table_schema.unique_constraints
            ]
        )
    else:
        lines.append("- Unique constraints beyond the primary key: none")
    lines.append(f"- Main logical refs: {logical_refs}")
    return "\n".join(lines)


def render_secondary_indexes_block(table_schema: TableSchema) -> str:
    """Render the schema-derived secondary-index section for one table page."""

    if not table_schema.secondary_indexes:
        return "- None beyond the primary key and unique constraints."
    return "\n".join(
        [
            f"- `{index.name} ({', '.join(index.columns)})`"
            + (f" WHERE {index.where_clause}" if index.where_clause is not None else "")
            for index in table_schema.secondary_indexes
        ]
    )


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
    order_description: Sequence[str],
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
            order_description=order_description,
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


def sync_table_schema_sections(
    path: Path,
    *,
    table_name: str,
    table_schema: TableSchema,
    incoming_foreign_keys: Sequence[IncomingForeignKeyMetadata],
    logical_refs: str,
) -> None:
    """Update one table page with generated schema-derived relationship data."""

    text = path.read_text(encoding="utf-8")
    text = _upsert_section(
        text,
        heading="Keys And Relationships",
        marker_name="generated_keys_and_relationships",
        content=render_keys_and_relationships_block(
            table_name=table_name,
            table_schema=table_schema,
            incoming_foreign_keys=incoming_foreign_keys,
            logical_refs=logical_refs,
        ),
        insert_before_heading="Secondary Indexes",
    )
    text = _upsert_section(
        text,
        heading="Secondary Indexes",
        marker_name="generated_secondary_indexes",
        content=render_secondary_indexes_block(table_schema),
        insert_before_heading="Main Read Paths",
    )
    path.write_text(text, encoding="utf-8")


def sync_table_doc_page(
    path: Path,
    *,
    table_name: str,
    stats: TableStats,
    table_schema: TableSchema,
    incoming_foreign_keys: Sequence[IncomingForeignKeyMetadata],
    logical_refs: str,
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
    sync_table_schema_sections(
        path,
        table_name=table_name,
        table_schema=table_schema,
        incoming_foreign_keys=incoming_foreign_keys,
        logical_refs=logical_refs,
    )
    sync_table_sample_rows(
        path,
        table_name=table_name,
        sample_rows=sample_rows,
        snapshot_date=snapshot_date,
        order_description=_sample_order_display(table_name, table_schema),
    )


def render_table_inventory_block(
    *,
    snapshot_date: str,
    stats_by_table: dict[str, TableStats],
    schema_by_table: Mapping[str, TableSchema],
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
            primary_key = schema_by_table[entry.table_name].primary_key
            pk_columns = (
                ", ".join(f"`{column}`" for column in primary_key)
                if primary_key
                else "`none`"
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


def sync_table_inventory_page(
    path: Path,
    *,
    snapshot_date: str,
    stats_by_table: dict[str, TableStats],
    schema_by_table: Mapping[str, TableSchema],
) -> None:
    """Update the table inventory page with generated live row counts and sizes."""

    text = path.read_text(encoding="utf-8")
    generated = render_table_inventory_block(
        snapshot_date=snapshot_date,
        stats_by_table=stats_by_table,
        schema_by_table=schema_by_table,
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
    order_by_table: Mapping[str, Sequence[str]],
) -> None:
    """Update the appendix page for wide-table samples."""

    generated = render_sample_rows_appendix(
        APPENDIX_SAMPLE_TABLES,
        samples_by_table,
        snapshot_date=snapshot_date,
        order_by_table=order_by_table,
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


def render_schema_snapshot(conn: sqlite3.Connection) -> str:
    """Render a deterministic SQL schema snapshot from sqlite_master."""

    rows = conn.execute(
        """
        SELECT type, name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
          AND type IN ('table', 'index')
          AND name NOT LIKE 'sqlite_%'
        ORDER BY CASE type WHEN 'table' THEN 0 ELSE 1 END, name
        """
    ).fetchall()
    statements = [str(row["sql"]).strip().rstrip(";") + ";" for row in rows]
    return "\n".join(statements) + "\n"


def sync_schema_snapshot(path: Path, *, schema_sql: str) -> None:
    """Write the generated schema snapshot SQL."""

    path.write_text(schema_sql, encoding="utf-8")


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
        schema_by_table = load_all_table_schemas(conn, TABLE_SEQUENCE)
        incoming_foreign_keys = build_incoming_foreign_keys(schema_by_table)
        order_by_table = {
            table_name: _sample_order_display(table_name, schema_by_table[table_name])
            for table_name in TABLE_SEQUENCE
        }
        samples_by_table = {
            table_name: fetch_sample_rows(
                conn,
                table_name,
                table_schema=schema_by_table[table_name],
            )
            for table_name in TABLE_SEQUENCE
        }
        stats_by_table = (
            {} if sample_rows_only else load_all_table_stats(conn, TABLE_SEQUENCE)
        )
        schema_snapshot_sql = "" if sample_rows_only else render_schema_snapshot(conn)
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
                order_description=order_by_table[table_name],
            )
        else:
            sync_table_doc_page(
                path,
                table_name=table_name,
                stats=stats_by_table[table_name],
                table_schema=schema_by_table[table_name],
                incoming_foreign_keys=incoming_foreign_keys[table_name],
                logical_refs=TABLE_ENTRY_BY_NAME[table_name].logical_refs,
                sample_rows=samples_by_table[table_name],
                snapshot_date=snapshot_date,
            )
    if not sample_rows_only:
        sync_table_inventory_page(
            docs_root / "table-inventory.md",
            snapshot_date=snapshot_date,
            stats_by_table=stats_by_table,
            schema_by_table=schema_by_table,
        )
    sync_sample_rows_appendix(
        docs_root / "sample-rows.md",
        snapshot_date=snapshot_date,
        samples_by_table=samples_by_table,
        order_by_table=order_by_table,
    )
    if not sample_rows_only:
        sync_schema_snapshot(
            docs_root / "schema.snapshot.sql",
            schema_sql=schema_snapshot_sql,
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
