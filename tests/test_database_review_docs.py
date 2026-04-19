"""Tests for database review documentation generation.

Author: OpenAI Codex
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pyvalue.database_review_docs as database_review_docs
from pyvalue.database_review_docs import (
    TableStats,
    fetch_sample_rows,
    generate_database_review_docs,
    render_sample_rows_block,
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


def test_fetch_sample_rows_uses_unordered_limit_and_truncates_large_values(
    tmp_path: Path,
) -> None:
    conn = _seed_example_db(tmp_path / "sample.db")
    statements: list[str] = []
    conn.set_trace_callback(statements.append)
    try:
        rows = fetch_sample_rows(conn, "example", truncate_chars=10)
    finally:
        conn.close()

    assert len(rows) == 3
    assert rows[0]["payload"] == "xxxxxxxxxx... <truncated; 32 bytes total>"
    assert {row["id"] for row in rows} == {1, 2, 3}
    select_statement = next(
        statement for statement in statements if 'FROM "example"' in statement
    )
    assert "ORDER BY" not in select_statement
    assert "LIMIT 5" in select_statement


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
        sample_rows=[{"id": 1, "name": "alpha"}],
        snapshot_date="2026-04-19",
    )

    text = page.read_text(encoding="utf-8")
    assert "<!-- BEGIN generated_live_stats -->" in text
    assert "- Row count: `3`" in text
    assert "## Sample Rows" in text
    assert "<!-- BEGIN generated_sample_rows -->" in text
    assert '"id": 1' in text
    assert "using `LIMIT` with no `ORDER BY`" in text
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

    stats_by_table = {
        "supported_exchanges": TableStats(1, 1024),
        "securities": TableStats(2, 2048),
        "supported_tickers": TableStats(3, 3072),
        "fundamentals_raw": TableStats(4, 4096),
        "fundamentals_fetch_state": TableStats(5, 5120),
        "security_listing_status": TableStats(6, 6144),
        "fundamentals_normalization_state": TableStats(7, 7168),
        "market_data_fetch_state": TableStats(8, 8192),
        "financial_facts": TableStats(9, 9216),
        "financial_facts_refresh_state": TableStats(10, 10240),
        "market_data": TableStats(11, 11264),
        "metrics": TableStats(12, 12288),
        "metric_compute_status": TableStats(13, 13312),
        "fx_supported_pairs": TableStats(14, 14336),
        "fx_refresh_state": TableStats(15, 15360),
        "fx_rates": TableStats(16, 16384),
        "schema_migrations": TableStats(1, 4096),
    }

    sync_table_inventory_page(
        inventory,
        snapshot_date="2026-04-19",
        stats_by_table=stats_by_table,
    )

    text = inventory.read_text(encoding="utf-8")
    assert "<!-- BEGIN generated_table_inventory -->" in text
    assert "[supported_exchanges](tables/supported_exchanges.md)" in text
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
        samples_by_table={"fundamentals_raw": [{"provider": "EODHD"}]},
    )

    text = appendix.read_text(encoding="utf-8")
    assert "## `fundamentals_raw`" in text
    assert '"provider": "EODHD"' in text
    assert "using `LIMIT` with no `ORDER BY`" in text
    assert "<!-- BEGIN generated_sample_rows_appendix -->" in text


def test_render_sample_rows_block_mentions_snapshot_and_count() -> None:
    block = render_sample_rows_block(
        "example",
        [{"id": 1}, {"id": 2}],
        snapshot_date="2026-04-19",
    )
    assert "Snapshot source: `data/pyvalue.db` on `2026-04-19`" in block
    assert "first `2` rows" in block
    assert "using `LIMIT` with no `ORDER BY`" in block


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
    assert inventory.read_text(encoding="utf-8") == "inventory stays\n"
