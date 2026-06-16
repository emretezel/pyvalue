"""Persistence layer: SQLite storage, schema migrations, and DB documentation.

This package groups every database-facing concern of pyvalue:
``storage`` -- the SQLite DAO/repository layer, which also houses the ordered
schema-migration runner (``migrations``) and the schema-doc generator
(``database_review_docs``); all database access lives under ``storage``.

Consumers import the concrete sub-modules directly — e.g.
``from pyvalue.persistence.storage import FactRecord`` — mirroring the
established ``pyvalue.marketdata.base`` / ``pyvalue.metrics.utils`` style.
``apply_migrations`` is surfaced here because it is the persistence layer's
primary verb (the storage layer calls it during schema initialisation).

Author: Emre Tezel
"""

from .storage.migrations import apply_migrations

__all__ = ["apply_migrations"]
