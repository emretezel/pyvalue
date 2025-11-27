"""GLEIF Golden Copy ingestion helpers for ISIN/LEI/company number mapping.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import requests
import gzip
import zipfile

LOGGER = logging.getLogger(__name__)

GOLDEN_URL = "https://goldencopy.gleif.org/api/v2/golden-copies/publishes/lei2/latest.csv"
ISIN_URL_TEMPLATE = "https://isinmapping.gleif.org/api/v2/isin-lei/{date}/download"
DEFAULT_TIMEOUT = 60


class GLEIFClient:
    """Download and parse GLEIF CSVs for ISIN/LEI/company number mapping."""

    DEFAULT_URL = GOLDEN_URL
    DEFAULT_ISIN_TEMPLATE = ISIN_URL_TEMPLATE

    def __init__(
        self,
        golden_url: str = GOLDEN_URL,
        isin_url_template: str = ISIN_URL_TEMPLATE,
        session: Optional[requests.Session] = None,
        golden_fetcher: Optional[callable] = None,
        isin_fetcher: Optional[callable] = None,
    ) -> None:
        self.golden_url = golden_url
        self.isin_url_template = isin_url_template
        self.session = session or requests.Session()
        self._golden_fetcher = golden_fetcher
        self._isin_fetcher = isin_fetcher

    def fetch_golden_csv(self, timeout: int = DEFAULT_TIMEOUT) -> str:
        if self._golden_fetcher is not None:
            return self._golden_fetcher()

        LOGGER.info("Downloading GLEIF golden copy from %s", self.golden_url)
        resp = self.session.get(self.golden_url, timeout=timeout)
        resp.raise_for_status()
        return self._decode_body(resp.content)

    def fetch_isin_csv(self, as_of: Optional[str] = None, timeout: int = DEFAULT_TIMEOUT) -> str:
        if self._isin_fetcher is not None:
            return self._isin_fetcher()

        date_str = as_of or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        url = self.isin_url_template.format(date=date_str)
        LOGGER.info("Downloading GLEIF ISIN mapping from %s", url)
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return self._decode_body(resp.content)

    def isin_to_company_number(self, golden_body: str, isin_body: str) -> Dict[str, Dict[str, str]]:
        """Parse CSV bodies to map ISIN -> {lei, company_number}."""

        lei_to_number = self._parse_golden(golden_body)
        isin_to_lei = self._parse_isin(isin_body)

        mapping: Dict[str, Dict[str, str]] = {}
        for isin, lei in isin_to_lei.items():
            company_number = lei_to_number.get(lei)
            if not company_number:
                continue
            mapping[isin] = {"lei": lei, "company_number": company_number}
        return mapping

    def _parse_golden(self, body: str) -> Dict[str, str]:
        text = self._strip_nuls(body)
        if not text:
            return {}
        reader = csv.DictReader(io.StringIO(text), delimiter=self._detect_delimiter(text))
        mapping: Dict[str, str] = {}
        uk_ra_codes = {
            "RA000407",  # Companies House
            "GB-COH",
            "RA000585",  # England/Wales
            "RA000586",  # Northern Ireland
            "RA000587",  # Scotland
        }

        for row in reader:
            ra_id = (
                row.get("Entity.RegistrationAuthority.RegistrationAuthorityID")
                or row.get("RegistrationAuthorityID")
                or row.get("Entity.RegistrationAuthority.AuthorityID")
                or row.get("RegistrationAuthority.AuthorityID")
                or ""
            ).strip()
            legal_jurisdiction = (
                row.get("Entity.LegalJurisdiction")
                or row.get("LegalJurisdiction")
                or ""
            ).strip()
            is_uk = ra_id in uk_ra_codes or legal_jurisdiction == "GB"
            if not is_uk:
                continue
            lei = (row.get("LEI") or "").strip()
            company_number = (
                row.get("Entity.RegistrationAuthority.RegistrationAuthorityEntityID")
                or row.get("RegistrationAuthorityEntityID")
                or row.get("Entity.RegistrationAuthority.EntityID")
                or row.get("RegistrationAuthority.EntityID")
                or ""
            ).strip()
            if not lei or not company_number:
                continue
            mapping[lei] = company_number
        return mapping

    def _parse_isin(self, body: str) -> Dict[str, str]:
        text = self._strip_nuls(body)
        if not text:
            return {}
        reader = csv.DictReader(io.StringIO(text), delimiter=self._detect_delimiter(text))
        mapping: Dict[str, str] = {}
        for row in reader:
            isin = (row.get("ISIN") or "").strip()
            lei = (row.get("LEI") or "").strip()
            if not isin or not lei:
                continue
            mapping[isin] = lei
        return mapping

    @staticmethod
    def _decode_body(raw: bytes) -> str:
        if raw.startswith(b"PK"):
            try:
                with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                    names = zf.namelist()
                    if not names:
                        return ""
                    raw = zf.read(names[0])
            except zipfile.BadZipFile:
                pass
        if raw.startswith(b"\x1f\x8b"):
            try:
                raw = gzip.decompress(raw)
            except OSError:
                pass
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("utf-8", errors="ignore")

    @staticmethod
    def _strip_nuls(text: str) -> str:
        return text.replace("\x00", "")

    @staticmethod
    def _detect_delimiter(text: str) -> str:
        sample = text[:2048]
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t"])
            return dialect.delimiter
        except Exception:
            return ","


__all__ = ["GLEIFClient"]
