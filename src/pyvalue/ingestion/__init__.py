"""Data ingestion helpers for various regulators.

Author: Emre Tezel
"""

from .sec import SECCompanyFactsClient
from .eodhd import EODHDFundamentalsClient
__all__ = ["SECCompanyFactsClient", "EODHDFundamentalsClient"]
