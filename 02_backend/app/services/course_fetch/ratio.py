"""Pick the closest-matching student-to-staff ratio for a framed course.

The stats graph holds one observation node per (institution, scope, year).
This module loads them once and, for each course, chooses:

1. the ISCED-F broad-field ratio(s) matching the course's own broad field(s) —
   averaged when the course spans several fields ETER covers;
2. failing that, the institution-wide ratio;
3. failing that, nothing at all.

Resolution happens at gold time rather than in silver: baking an
institution-level figure into per-course triples would mean re-running
bronze→silver for every provider each time ETER publishes, whereas here an
ETER refresh is just `course reindex --all`.
"""

import logging
from typing import Any, Dict, List, Optional

import requests
from rdflib.namespace import DCTERMS

from config import GRAPH_STATS
from services import fuseki

logger = logging.getLogger(__name__)

QL = "http://data.quality-link.eu/ontology/v1#"

# provider IRI -> broad-field IRI (or None for institution-wide) -> observation
RatioIndex = Dict[str, Dict[Optional[str], Dict[str, Any]]]

_RATIO_INDEX: Optional[RatioIndex] = None

# text labels
SOURCE_LABEL = "EHESO"
INST_LABEL = "institution-wide"
ISCEDF_LABEL = "ISCED-F broad field"


def clear_ratio_cache() -> None:
    """Drop the in-process index so the next resolve reloads from Fuseki."""
    global _RATIO_INDEX
    _RATIO_INDEX = None


def load_ratio_index(
    session: Optional[requests.Session] = None, *, refresh: bool = False
) -> RatioIndex:
    """Load every ratio observation, keeping the latest year per scope.

    Keyed by `ql:aboutOrganisation`, and resolved per institution *and* per
    scope — so an institution whose field breakdown stopped in 2020 still gets
    its 2022 institution-wide figure rather than being dragged back to 2020.
    """
    global _RATIO_INDEX
    if _RATIO_INDEX is not None and not refresh:
        return _RATIO_INDEX

    query = f"""
PREFIX ql: <{QL}>
PREFIX dcterms: <{DCTERMS}>

SELECT ?org ?field ?value ?year ?source
FROM <{GRAPH_STATS}>
WHERE {{
  ?ratio a ql:StudentStaffRatio ;
         ql:aboutOrganisation ?org ;
         ql:ratioValue ?value ;
         ql:referenceYear ?year .
  OPTIONAL {{ ?ratio ql:ISCEDFBroadField ?field }}
  OPTIONAL {{ ?ratio dcterms:source ?source }}
}}
"""
    index: RatioIndex = {}
    for binding in fuseki.sparql_select(query, session=session):
        org = binding.get("org", {}).get("value")
        raw_value = binding.get("value", {}).get("value")
        raw_year = binding.get("year", {}).get("value")
        if not org or raw_value is None or raw_year is None:
            continue
        try:
            value = float(raw_value)
            year = int(str(raw_year)[:4])
        except (TypeError, ValueError):
            continue

        scope = binding.get("field", {}).get("value")  # None => institution-wide
        scopes = index.setdefault(org, {})
        existing = scopes.get(scope)
        if existing is None or year > existing["year"]:
            scopes[scope] = {
                "value": value,
                "year": year,
                "source": binding.get("source", {}).get("value"),
            }

    _RATIO_INDEX = index
    logger.info("Loaded student-staff ratios for %s institution(s)", len(index))
    return index


def _iri_values(value: Any) -> List[str]:
    """Normalise a framed `@type: @id` term into a list of IRI strings.

    Framing can produce any of four shapes depending on whether the vocabulary
    graph has a label for the concept and whether the course has one value or
    several: a bare IRI string, a node object with `id`, or a list of either.
    """
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        node_id = value.get("id") or value.get("@id")
        return [node_id] if isinstance(node_id, str) else []
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(_iri_values(item))
        return out
    return []


def _publisher_iri(framed: Dict[str, Any]) -> Optional[str]:
    candidates = _iri_values(framed.get("dcterms:publisher"))
    return candidates[0] if candidates else None


def resolve_student_staff_ratio(
    framed: Dict[str, Any],
    index: Optional[RatioIndex] = None,
    *,
    session: Optional[requests.Session] = None,
) -> Optional[Dict[str, Any]]:
    """Return the ratio doc field for a framed course, or None if unavailable."""
    if index is None:
        index = load_ratio_index(session)

    publisher = _publisher_iri(framed)
    if not publisher:
        return None

    scopes = index.get(publisher)
    if not scopes:
        return None

    broad_fields = _iri_values(framed.get("ISCEDFBroadField"))
    matched = [(f, scopes[f]) for f in dict.fromkeys(broad_fields) if f in scopes]

    if matched:
        values = [entry["value"] for _, entry in matched]
        # `averaged` and `fields` let the catalogue label a multi-field mean as
        # derived rather than present it as a published ETER figure.
        return {
            "value": round(sum(values) / len(values), 2),
            "scope": ISCEDF_LABEL,
            "fields": [f for f, _ in matched],
            "referenceYear": max(entry["year"] for _, entry in matched),
            "averaged": len(matched) > 1,
            "source": SOURCE_LABEL,
        }

    institution = scopes.get(None)
    if institution is None:
        return None

    return {
        "value": round(institution["value"], 2),
        "scope": INST_LABEL,
        "referenceYear": institution["year"],
        "source": SOURCE_LABEL,
    }
