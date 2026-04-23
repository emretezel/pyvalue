"""Tests for database review documentation generation.

Author: OpenAI Codex
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pyvalue.database_review_docs as database_review_docs
from pyvalue.database_review_docs import (
    TableInventoryEntry,
    TableSchema,
    TableStats,
    build_incoming_foreign_keys,
    fetch_sample_rows,
    generate_database_review_docs,
    load_table_schema,
    render_keys_and_relationships_block,
    render_sample_rows_block,
    render_schema_snapshot,
    render_secondary_indexes_block,
    sync_sample_rows_appendix,
    sync_table_sample_rows,
    sync_table_doc_page,
    sync_table_inventory_page,
)


def _seed_example_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE example (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            payload TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO example (id, name, payload) VALUES (?, ?, ?)",
        [
            (3, "gamma", "short"),
            (1, "alpha", "x" * 32),
            (2, "beta", "middle"),
        ],
    )
    conn.commit()
    return conn


def _seed_relationship_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE parent (id INTEGER PRIMARY KEY, code TEXT NOT NULL UNIQUE)"
    )
    conn.execute(
        """
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            category TEXT,
            FOREIGN KEY(parent_id) REFERENCES parent(id),
            UNIQUE (parent_id, symbol)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_child_category_nonnull
        ON child(category)
        WHERE category IS NOT NULL
        """
    )
    conn.execute(
        """
        CREATE INDEX idx_child_parent_desc
        ON child(parent_id, id DESC)
        """
    )
    conn.execute("CREATE TABLE no_pk (name TEXT NOT NULL)")
    conn.executemany(
        "INSERT INTO no_pk (name) VALUES (?)",
        [("second",), ("first",)],
    )
    conn.commit()
    return conn


def test_fetch_sample_rows_orders_by_primary_key_and_truncates_large_values(
    tmp_path: Path,
) -> None:
    conn = _seed_example_db(tmp_path / "sample.db")
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        rows = fetch_sample_rows(
            conn,
            "example",
            table_schema=load_table_schema(conn, "example"),
            truncate_chars=10,
        )
    finally:
        conn.close()

    assert len(rows) == 3
    assert rows[0]["payload"] == "xxxxxxxxxx... <truncated; 32 bytes total>"
    assert [row["id"] for row in rows] == [1, 2, 3]
    select_statement = next(
        statement for statement in statements if 'FROM "example"' in statement
    )
    assert 'ORDER BY "id" ASC' in select_statement
    assert "LIMIT 5" in select_statement


def test_fetch_sample_rows_falls_back_to_rowid_when_no_primary_key(
    tmp_path: Path,
) -> None:
    conn = _seed_relationship_db(tmp_path / "sample.db")
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        rows = fetch_sample_rows(
            conn,
            "no_pk",
            table_schema=load_table_schema(conn, "no_pk"),
        )
    finally:
        conn.close()

    assert [row["name"] for row in rows] == ["second", "first"]
    select_statement = next(
        statement for statement in statements if 'FROM "no_pk"' in statement
    )
    assert "ORDER BY rowid ASC" in select_statement


def test_render_schema_blocks_include_pk_fk_unique_and_secondary_indexes(
    tmp_path: Path,
) -> None:
    conn = _seed_relationship_db(tmp_path / "schema.db")
    try:
        schema_by_table = {
            "parent": load_table_schema(conn, "parent"),
            "child": load_table_schema(conn, "child"),
        }
        incoming = build_incoming_foreign_keys(schema_by_table)
        parent_block = render_keys_and_relationships_block(
            table_name="parent",
            table_schema=schema_by_table["parent"],
            incoming_foreign_keys=incoming["parent"],
            logical_refs="none",
        )
        child_block = render_keys_and_relationships_block(
            table_name="child",
            table_schema=schema_by_table["child"],
            incoming_foreign_keys=incoming["child"],
            logical_refs="none",
        )
        secondary_block = render_secondary_indexes_block(schema_by_table["child"])
    finally:
        conn.close()

    assert "- Primary key: `id`" in parent_block
    assert "- Physical references from other tables:" in parent_block
    assert "  - `child`.`parent_id` -> `id`" in parent_block
    assert "- Unique constraints beyond the primary key:" in parent_block
    assert "  - `code`" in parent_block
    assert "- Physical foreign keys:" in child_block
    assert "  - `parent_id` -> `parent`.`id`" in child_block
    assert "  - (`parent_id`, `symbol`)" in child_block
    assert (
        "`idx_child_category_nonnull (category)` WHERE category IS NOT NULL"
        in secondary_block
    )
    assert "`idx_child_parent_desc (parent_id, id DESC)`" in secondary_block
    assert "sqlite_autoindex" not in secondary_block


def test_sync_table_doc_page_replaces_generated_live_stats_and_adds_samples(
    tmp_path: Path,
) -> None:
    page = tmp_path / "example.md"
    page.write_text(
        "\n".join(
            [
                "# `example`",
                "",
                "## Purpose",
                "",
                "Example table.",
                "",
                "## Grain",
                "",
                "One row per example.",
                "",
                "## Live Stats",
                "",
                "stale",
                "",
                "## Columns",
                "",
                "| Column | Type |",
                "| --- | --- |",
                "| `id` | `INTEGER` |",
                "",
                "## Review Notes",
                "",
                "- note",
                "",
            ]
        ),
        encoding="utf-8",
    )

    sync_table_doc_page(
        page,
        table_name="example",
        stats=TableStats(row_count=3, size_bytes=1024),
        table_schema=TableSchema(
            primary_key=("id",),
            foreign_keys=(),
            unique_constraints=(),
            secondary_indexes=(),
        ),
        incoming_foreign_keys=(),
        logical_refs="none",
        sample_rows=[{"id": 1, "name": "alpha"}],
        snapshot_date="2026-04-19",
    )

    text = page.read_text(encoding="utf-8")
    assert "<!-- BEGIN generated_live_stats -->" in text
    assert "- Row count: `3`" in text
    assert "<!-- BEGIN generated_keys_and_relationships -->" in text
    assert "<!-- BEGIN generated_secondary_indexes -->" in text
    assert "## Sample Rows" in text
    assert "<!-- BEGIN generated_sample_rows -->" in text
    assert '"id": 1' in text
    assert "ordered by `id ASC`" in text
    assert text.index("## Sample Rows") < text.index("## Review Notes")


def test_sync_table_doc_page_links_appendix_for_wide_tables(tmp_path: Path) -> None:
    page = tmp_path / "fundamentals_raw.md"
    page.write_text(
        "\n".join(
            [
                "# `fundamentals_raw`",
                "",
                "## Purpose",
                "",
                "Raw payloads.",
                "",
                "## Grain",
                "",
                "One row per provider symbol.",
                "",
                "## Live Stats",
                "",
                "stale",
                "",
                "## Columns",
                "",
                "| Column | Type |",
                "| --- | --- |",
                "| `provider` | `TEXT` |",
                "",
                "## Review Notes",
                "",
                "- note",
                "",
            ]
        ),
        encoding="utf-8",
    )

    sync_table_doc_page(
        page,
        table_name="fundamentals_raw",
        stats=TableStats(row_count=1, size_bytes=2048),
        table_schema=TableSchema(
            primary_key=("payload_id",),
            foreign_keys=(),
            unique_constraints=(("provider_listing_id",),),
            secondary_indexes=(),
        ),
        incoming_foreign_keys=(),
        logical_refs="none",
        sample_rows=[{"provider": "EODHD"}],
        snapshot_date="2026-04-19",
    )

    text = page.read_text(encoding="utf-8")
    assert "[Sample Rows appendix](../sample-rows.md#fundamentals_raw)" in text
    assert '"provider": "EODHD"' not in text


def test_sync_table_inventory_page_replaces_generated_body(tmp_path: Path) -> None:
    inventory = tmp_path / "table-inventory.md"
    inventory.write_text(
        "# Table Inventory\n\nIntro.\n\n## Identity And Catalog\n\nold\n",
        encoding="utf-8",
    )

    stats_by_table = {"example": TableStats(1, 1024)}
    schema_by_table = {
        "example": TableSchema(
            primary_key=("id",),
            foreign_keys=(),
            unique_constraints=(),
            secondary_indexes=(),
        )
    }
    original_groups = database_review_docs.TABLE_GROUPS
    original_entry_map = database_review_docs.TABLE_ENTRY_BY_NAME
    database_review_docs.TABLE_GROUPS = (
        (
            "Identity And Catalog",
            (
                TableInventoryEntry(
                    table_name="example",
                    logical_refs="none",
                    review_focus="example focus",
                ),
            ),
        ),
    )
    database_review_docs.TABLE_ENTRY_BY_NAME = {
        "example": TableInventoryEntry(
            table_name="example",
            logical_refs="none",
            review_focus="example focus",
        )
    }

    try:
        sync_table_inventory_page(
            inventory,
            snapshot_date="2026-04-19",
            stats_by_table=stats_by_table,
            schema_by_table=schema_by_table,
        )
    finally:
        database_review_docs.TABLE_GROUPS = original_groups
        database_review_docs.TABLE_ENTRY_BY_NAME = original_entry_map

    text = inventory.read_text(encoding="utf-8")
    assert "<!-- BEGIN generated_table_inventory -->" in text
    assert "[example](tables/example.md)" in text
    assert "`1.0 KiB`" in text


def test_sync_table_sample_rows_preserves_existing_live_stats(tmp_path: Path) -> None:
    page = tmp_path / "example.md"
    page.write_text(
        "\n".join(
            [
                "# `example`",
                "",
                "## Purpose",
                "",
                "Example table.",
                "",
                "## Grain",
                "",
                "One row per example.",
                "",
                "## Live Stats",
                "",
                "<!-- BEGIN generated_live_stats -->",
                "keep me",
                "<!-- END generated_live_stats -->",
                "",
                "## Columns",
                "",
                "| Column | Type |",
                "| --- | --- |",
                "| `id` | `INTEGER` |",
                "",
                "## Sample Rows",
                "",
                "<!-- BEGIN generated_sample_rows -->",
                "old sample",
                "<!-- END generated_sample_rows -->",
                "",
                "## Review Notes",
                "",
                "- note",
                "",
            ]
        ),
        encoding="utf-8",
    )

    sync_table_sample_rows(
        page,
        table_name="example",
        sample_rows=[{"id": 7}],
        snapshot_date="2026-04-19",
        order_description=("id ASC",),
    )

    text = page.read_text(encoding="utf-8")
    assert "keep me" in text
    assert '"id": 7' in text
    assert "old sample" not in text


def test_sync_sample_rows_appendix_renders_wide_table_section(tmp_path: Path) -> None:
    appendix = tmp_path / "sample-rows.md"
    appendix.write_text(
        "# Sample Rows Appendix\n\nintro\n\n<!-- BEGIN generated_sample_rows_appendix -->\nold\n<!-- END generated_sample_rows_appendix -->\n",
        encoding="utf-8",
    )

    sync_sample_rows_appendix(
        appendix,
        snapshot_date="2026-04-19",
        samples_by_table={
            "fundamentals_raw": [
                {"provider": "EODHD", "data": "<omitted>", "data_bytes": 123}
            ]
        },
        order_by_table={"fundamentals_raw": ("payload_id ASC",)},
    )

    text = appendix.read_text(encoding="utf-8")
    assert "## `fundamentals_raw`" in text
    assert '"provider": "EODHD"' in text
    assert '"data_bytes": 123' in text
    assert "ordered by `payload_id ASC`" in text
    assert "payload size metadata" in text
    assert "<!-- BEGIN generated_sample_rows_appendix -->" in text


def test_render_sample_rows_block_mentions_snapshot_and_count() -> None:
    block = render_sample_rows_block(
        "example",
        [{"id": 1}, {"id": 2}],
        snapshot_date="2026-04-19",
        order_description=("id ASC",),
    )
    assert "Snapshot source: `data/pyvalue.db` on `2026-04-19`" in block
    assert "first `2` rows" in block
    assert "ordered by `id ASC`" in block


def test_render_schema_snapshot_emits_live_ddl(tmp_path: Path) -> None:
    conn = _seed_example_db(tmp_path / "sample.db")
    try:
        schema_sql = render_schema_snapshot(conn)
    finally:
        conn.close()

    assert "CREATE TABLE example" in schema_sql
    assert "sqlite_autoindex" not in schema_sql


def test_generate_database_review_docs_sample_rows_only_preserves_inventory(
    tmp_path: Path,
    monkeypatch,
) -> None:
    db_path = tmp_path / "sample.db"
    conn = _seed_example_db(db_path)
    conn.close()

    docs_root = tmp_path / "docs"
    tables_dir = docs_root / "tables"
    tables_dir.mkdir(parents=True)
    (tables_dir / "example.md").write_text(
        "\n".join(
            [
                "# `example`",
                "",
                "## Purpose",
                "",
                "Example table.",
                "",
                "## Grain",
                "",
                "One row per example.",
                "",
                "## Live Stats",
                "",
                "<!-- BEGIN generated_live_stats -->",
                "keep stats",
                "<!-- END generated_live_stats -->",
                "",
                "## Columns",
                "",
                "| Column | Type |",
                "| --- | --- |",
                "| `id` | `INTEGER` |",
                "",
                "## Review Notes",
                "",
                "- note",
                "",
            ]
        ),
        encoding="utf-8",
    )
    inventory = docs_root / "table-inventory.md"
    inventory.write_text("inventory stays\n", encoding="utf-8")
    (docs_root / "sample-rows.md").write_text(
        "# Sample Rows Appendix\n\n<!-- BEGIN generated_sample_rows_appendix -->\nold\n<!-- END generated_sample_rows_appendix -->\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(database_review_docs, "TABLE_SEQUENCE", ("example",))
    monkeypatch.setattr(database_review_docs, "APPENDIX_SAMPLE_TABLES", frozenset())

    generate_database_review_docs(
        database_path=db_path,
        docs_root=docs_root,
        sample_rows_only=True,
    )

    text = (tables_dir / "example.md").read_text(encoding="utf-8")
    assert "keep stats" in text
    assert '"id": 1' in text or '"id": 2' in text or '"id": 3' in text
    assert "ordered by `id ASC`" in text
    assert inventory.read_text(encoding="utf-8") == "inventory stays\n"
