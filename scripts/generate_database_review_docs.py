"""Wrapper script for refreshing checked-in database review docs.

Author: OpenAI Codex
"""

from __future__ import annotations

import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))


def _main() -> int:
    # The module moved into the storage package when all database access was
    # consolidated under persistence/storage/.
    from pyvalue.persistence.storage.database_review_docs import main

    return main()


if __name__ == "__main__":
    raise SystemExit(_main())
