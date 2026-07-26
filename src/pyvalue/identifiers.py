"""Security and legal-entity identifier normalization (ISIN, LEI).

Author: Emre Tezel

These are the identifiers that let the catalog reason about *which security* and
*which company* a listing belongs to, as opposed to how it is quoted:

* **ISIN** (ISO 6166) identifies a security. Every venue trading the same shares
  publishes the same ISIN, which is what makes it the grouping key for
  cross-listings -- ``ASML.AS`` and ``ASME.DU`` share ``NL0010273215``. A
  depositary receipt is legally a *different* security and therefore carries its
  own ISIN, so ISIN deliberately does **not** group an ADR with its underlying.
* **LEI** (ISO 17442) identifies the legal entity, so it groups every security a
  company issues, including separate share classes.

Both normalizers mirror :func:`pyvalue.currency.shaped_currency_code`: strip and
uppercase, then keep the value only when it matches the shape the database CHECK
constraints enforce, otherwise return ``None`` so writers store NULL rather than
tripping the constraint. The SQL predicates in
``persistence/storage/migrations.py`` (``_ISIN_FORMAT_CHECK``,
``_LEI_FORMAT_CHECK``) encode the identical rules -- keep the two in step.

Neither function verifies the ISIN check digit or the LEI checksum. The provider
is the authority on the value; these guards exist to reject structurally
impossible input (empty strings, placeholders, truncated codes) so a vendor
oddity degrades to "no identifier" instead of corrupting the peer groups.
"""

from __future__ import annotations

import re
from typing import Final, Optional

# 2-letter country prefix, 9-character national identifier, numeric check digit.
_ISIN_SHAPE: Final[re.Pattern[str]] = re.compile(r"[A-Z]{2}[A-Z0-9]{9}[0-9]")

# 20 uppercase alphanumerics; the final two are the ISO 17442 checksum digits,
# which are not verified here (see the module docstring).
_LEI_SHAPE: Final[re.Pattern[str]] = re.compile(r"[A-Z0-9]{20}")


def _normalized_identifier(value: object) -> Optional[str]:
    """Strip and uppercase ``value``, returning ``None`` when it holds no text.

    Providers publish identifiers as JSON strings but occasionally as ``null``,
    an empty string, or padded text, so normalization happens once here rather
    than at each call site.
    """

    if value is None:
        return None
    text = str(value).strip().upper()
    return text or None


def shaped_isin(value: object) -> Optional[str]:
    """Return ``value`` as a schema-shaped ISIN, or ``None``.

    Args:
        value: Raw provider value -- typically ``General.ISIN`` from an EODHD
            fundamentals payload or ``Isin`` from an exchange symbol list.

    Returns:
        The normalized 12-character ISIN when ``value`` matches ISO 6166's
        structure, otherwise ``None``. ``None`` is the correct stored value for
        a listing whose ISIN the provider does not publish -- absence is a valid
        state, not an error.
    """

    code = _normalized_identifier(value)
    if code is None or _ISIN_SHAPE.fullmatch(code) is None:
        return None
    return code


def shaped_lei(value: object) -> Optional[str]:
    """Return ``value`` as a schema-shaped LEI, or ``None``.

    Args:
        value: Raw provider value -- typically ``General.LEI`` from an EODHD
            fundamentals payload.

    Returns:
        The normalized 20-character LEI when ``value`` matches ISO 17442's
        structure, otherwise ``None``. EODHD publishes an LEI for roughly a
        quarter of listings, so ``None`` is the common, expected result.
    """

    code = _normalized_identifier(value)
    if code is None or _LEI_SHAPE.fullmatch(code) is None:
        return None
    return code


__all__ = ["shaped_isin", "shaped_lei"]
