"""Test configuration helpers.

Author: Emre Tezel
"""

import sys
from pathlib import Path

# Ensure the src/ directory is importable when running tests without installation.
ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def seed_exchange(
    db_path: Path,
    *codes: str,
    provider: str = "EODHD",
    currency: str = "USD",
) -> None:
    """Seed one or more existing provider_exchange rows for tests.

    The exchange catalog (``exchange`` + ``provider_exchange``) is owned by
    ``refresh-supported-exchanges``. ``replace_for_exchange`` /
    ``replace_from_listings`` now resolve the provider_exchange read-only and
    raise if it is absent, so any test that catalogs listings must seed the
    exchange first. Each upsert is non-destructive (it never prunes siblings),
    so a test can seed every exchange code it uses in a single call. Defaults to
    ``"US"`` when no code is given; the canonical exchange code mirrors the
    provider exchange code.
    """
    # Imported lazily so the src/ path insert above has already run.
    from pyvalue.persistence.storage import ExchangeProviderRepository

    repo = ExchangeProviderRepository(db_path)
    for code in codes or ("US",):
        repo.ensure_fixed_exchange(provider, code, code, currency=currency)
