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
    from pyvalue.database_review_docs import main

    return main()


if __name__ == "__main__":
    raise SystemExit(_main())
