"""ETER (European Tertiary Education Register) student-to-staff ratios.

Fetches student and academic-staff counts from the ETER API, derives
student-to-staff ratios at institution level and per ISCED-F broad field, and
pushes them into the Fuseki stats graph as standalone observation nodes.

Deliberately no Postgres table: the raw API payload is cached in MinIO (so a
re-derive never needs the API) and the derived ratios live in Fuseki (so what
you can query is exactly what serves the pipeline). Postgres is only read, once
per run, to map ETER ids onto provider IRIs.
"""

import io
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from minio import Minio
from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, RDF, XSD
from sqlalchemy import text
from sqlalchemy.orm import Session

from config import (
    ETER_API_TIMEOUT,
    ETER_API_URL,
    GRAPH_STATS,
    MINIO_BUCKET_NAME,
)
from services import fuseki

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")

# Provenance IRI recorded on every node, and the key the purge scopes on so a
# future non-ETER stats source publishing ql:StudentStaffRatio is left alone.
ETER_SOURCE_URI = URIRef(
    "https://national-policies.eacea.ec.europa.eu/eheso/micro-data-access"
)

ISCED_F_BASE = "http://data.europa.eu/snb/isced-f/"
STATS_IRI_BASE = "https://data.quality-link.eu/stats/eter/"

MINIO_PREFIX = "eter/"

# ETER field ids. `FOE{nn}` suffixes are ISCED-F broad fields 00..10.
FIELD_ETER_ID = "BAS.ETERID"
FIELD_STAFF_FTE_TOTAL = "PERS.ACAFTETOTAL"
FIELD_STAFF_HC_TOTAL = "PERS.TOTACAHC"
# Underscore, not hyphen — `STUD.TOTALISCED5-7` is silently dropped from the
# response rather than erroring, so a typo here yields zero institution ratios.
FIELD_STUDENTS_TOTAL = "STUD.TOTALISCED5_7"

BROAD_FIELDS = [f"{n:02d}" for n in range(0, 11)]  # "00".."10"

# ETER allocates students by *programme* field but staff by the staff member's
# own field, so the two breakdowns do not correspond wherever one department
# teaches another's students. Left unguarded this yields absurdities: ES0059
# reports 2 staff against 6,674 students in field 09 (a 5,444:1 "ratio") while
# its institution-wide figure is a sane 20:1. Two guards, both sized against
# the full 2022 population (3,439 institutions):
#
# - Breakdown coherence: if the per-field staff headcounts sum to less than
#   this share of total academic headcount, the breakdown is too incomplete to
#   divide by at all. 80% keeps 712 of 782 institutions that report any field
#   staff; the median institution sits at 98.6%.
# - Deviation from the institution ratio: a field ratio is meant to *refine*
#   the institution-wide figure, so an order-of-magnitude departure signals
#   mismatched allocations rather than real teaching intensity. A factor of 5
#   retains 92% of field rows, comfortably wider than genuine cross-field
#   variation (medicine vs law).
#
# Institution-wide ratios get neither guard: they are a faithful division of
# two reported totals, and the extremes are real (Anadolu's 1,247:1 reflects an
# actual mega open university, not a data error).
MIN_STAFF_BREAKDOWN_COVERAGE = 0.8
MAX_FIELD_DEVIATION = 5.0


def _staff_hc_field(broad: str) -> str:
    return f"PERS.ACAHCFOE{broad}"


def _students_field(broad: str) -> str:
    return f"STUD.ISCED5_7FOE{broad}"


def requested_field_ids() -> List[str]:
    """Every ETER field id the ratio computation needs."""
    ids = [
        FIELD_ETER_ID,
        FIELD_STAFF_FTE_TOTAL,
        FIELD_STAFF_HC_TOTAL,
        FIELD_STUDENTS_TOTAL,
    ]
    for broad in BROAD_FIELDS:
        ids.append(_staff_hc_field(broad))
        ids.append(_students_field(broad))
    return ids


@dataclass
class RatioRow:
    """One derived observation: an institution, a scope, and a ratio."""

    eter_id: str
    isced_f_broad: Optional[str]  # None = institution-wide
    reference_year: int
    students: float
    staff: float
    ratio: float
    source_variable: str


@dataclass
class FetchStats:
    records: int = 0
    ratios: int = 0
    institution_scope: int = 0
    field_scope: int = 0
    matched_ids: int = 0
    unmatched_ids: List[str] = field(default_factory=list)
    dropped_incoherent: int = 0
    dropped_implausible: int = 0
    pushed: int = 0
    failed: int = 0


# --------------------------------------------------------------------------
# API client
# --------------------------------------------------------------------------

def _query_url() -> str:
    return f"{ETER_API_URL.rstrip('/')}/HEIs/query/flattened"


def fetch_eter_records(
    *,
    year: int,
    countries: Optional[Iterable[str]] = None,
    session: Optional[requests.Session] = None,
    timeout: int = ETER_API_TIMEOUT,
) -> Tuple[List[Dict[str, Any]], bytes]:
    """POST one query to the ETER API. Returns (records, raw_response_bytes).

    Open data only — no Authorization header. A Bearer token from the ETER
    /login endpoint would unlock the restricted dataset if that is ever needed.

    Never raises: on failure returns ([], b"") and logs, matching
    `deqar.fetch_deqar_providers`.
    """
    body: Dict[str, Any] = {
        "filter": {"BAS.REFYEAR.v": year},
        "fieldIds": requested_field_ids(),
        "searchTerms": [],
    }
    countries = list(countries or [])
    if countries:
        body["filter"]["BAS.COUNTRY.v"] = (
            countries[0] if len(countries) == 1 else {"$in": countries}
        )

    url = _query_url()
    logger.info("Fetching ETER year=%s countries=%s from %s", year, countries or "all", url)

    http = session or requests
    try:
        response = http.post(
            url,
            json=body,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            timeout=timeout,
        )
    except requests.RequestException as e:
        logger.error("ETER request failed: %s", e)
        return [], b""

    # The query endpoint answers 201, not 200.
    if not 200 <= response.status_code < 300:
        logger.error("ETER HTTP %s: %s", response.status_code, response.text[:500])
        return [], b""

    raw = response.content
    try:
        payload = response.json()
    except ValueError as e:
        logger.error("ETER returned invalid JSON: %s", e)
        return [], b""

    records = _extract_records(payload)
    logger.info("ETER fetch done: %s record(s)", len(records))
    return records, raw


def _extract_records(payload: Any) -> List[Dict[str, Any]]:
    """Pull the record list out of the response, whatever envelope it uses."""
    if isinstance(payload, list):
        return [r for r in payload if isinstance(r, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "hits", "docs"):
            value = payload.get(key)
            if isinstance(value, list):
                return [r for r in value if isinstance(r, dict)]
        logger.warning("ETER: unrecognised response envelope, keys=%s", list(payload)[:10])
    return []


# --------------------------------------------------------------------------
# MinIO cache
# --------------------------------------------------------------------------

def cache_raw(minio_client: Minio, raw: bytes, *, year: int) -> Optional[str]:
    """Store the untransformed API response. Returns the object key."""
    if not raw:
        return None
    if not minio_client.bucket_exists(MINIO_BUCKET_NAME):
        minio_client.make_bucket(MINIO_BUCKET_NAME)
        logger.info("Created MinIO bucket %s", MINIO_BUCKET_NAME)

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = f"{MINIO_PREFIX}{today}/heis-{year}.json"
    try:
        minio_client.put_object(
            MINIO_BUCKET_NAME,
            key,
            io.BytesIO(raw),
            length=len(raw),
            content_type="application/json",
        )
    except Exception as e:
        logger.error("Failed to cache ETER payload at %s: %s", key, e)
        return None
    logger.info("Cached ETER payload (%s bytes) at %s", len(raw), key)
    return key


def load_cached_records(
    minio_client: Minio, *, year: int, key: Optional[str] = None
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Read back a cached payload — the newest for `year` unless `key` is given."""
    if key is None:
        try:
            objects = minio_client.list_objects(
                MINIO_BUCKET_NAME, prefix=MINIO_PREFIX, recursive=True
            )
            candidates = [
                o.object_name
                for o in objects
                if o.object_name.endswith(f"heis-{year}.json")
            ]
        except Exception as e:
            logger.error("Failed to list cached ETER payloads: %s", e)
            return [], None
        if not candidates:
            logger.error("No cached ETER payload for year %s under %s", year, MINIO_PREFIX)
            return [], None
        # Keys embed an ISO date, so lexical max is the newest.
        key = max(candidates)

    try:
        response = minio_client.get_object(MINIO_BUCKET_NAME, key)
        try:
            raw = response.read()
        finally:
            response.close()
            response.release_conn()
    except Exception as e:
        logger.error("Failed to read cached ETER payload %s: %s", key, e)
        return [], None

    try:
        payload = json.loads(raw)
    except ValueError as e:
        logger.error("Cached ETER payload %s is not valid JSON: %s", key, e)
        return [], None

    records = _extract_records(payload)
    logger.info("Loaded %s cached ETER record(s) from %s", len(records), key)
    return records, key


# --------------------------------------------------------------------------
# Ratio computation
# --------------------------------------------------------------------------

def _get(record: Dict[str, Any], field_id: str) -> Any:
    """Read a field from a flattened ETER record.

    The flattened form uses either the bare field id or a `.v` suffix for the
    value; accept both rather than betting on one.
    """
    if field_id in record:
        return record[field_id]
    return record.get(f"{field_id}.v")


def _num(value: Any) -> Optional[float]:
    """Strict numeric coercion.

    ETER encodes missing/confidential values as flag strings ("a", "m", "nc",
    "x", "xc", ...). Those must drop out, never be read as zero — a zero
    numerator would silently publish a ratio of 0.0.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip().replace(",", "")
        if not stripped:
            return None
        try:
            return float(stripped)
        except ValueError:
            return None
    return None


def _ratio_row(
    *,
    eter_id: str,
    broad: Optional[str],
    year: int,
    students: Optional[float],
    staff: Optional[float],
    source_variable: str,
) -> Optional[RatioRow]:
    if students is None or staff is None:
        return None
    if students <= 0:
        return None
    # Denominators below 1 FTE produce meaningless ratios (a 0.2-FTE
    # denominator turns 40 students into 200:1).
    if staff < 1:
        return None
    return RatioRow(
        eter_id=eter_id,
        isced_f_broad=broad,
        reference_year=year,
        students=students,
        staff=staff,
        ratio=round(students / staff, 2),
        source_variable=source_variable,
    )


def compute_ratios(
    records: List[Dict[str, Any]], *, year: int, stats: Optional[FetchStats] = None
) -> List[RatioRow]:
    """Derive institution-wide and per-broad-field ratios from ETER records."""
    rows: List[RatioRow] = []
    dropped_incoherent = 0
    dropped_implausible = 0

    for record in records:
        eter_id = _get(record, FIELD_ETER_ID)
        if not isinstance(eter_id, str) or not eter_id.strip():
            continue
        eter_id = eter_id.strip()

        students_total = _num(_get(record, FIELD_STUDENTS_TOTAL))
        staff_fte_total = _num(_get(record, FIELD_STAFF_FTE_TOTAL))
        staff_hc_total = _num(_get(record, FIELD_STAFF_HC_TOTAL))

        # Institution-wide: FTE is the meaningful denominator; fall back to
        # headcount when ETER has no FTE for this institution.
        if staff_fte_total is not None:
            institution_staff, staff_basis = staff_fte_total, FIELD_STAFF_FTE_TOTAL
        else:
            institution_staff, staff_basis = staff_hc_total, FIELD_STAFF_HC_TOTAL

        row = _ratio_row(
            eter_id=eter_id,
            broad=None,
            year=year,
            students=students_total,
            staff=institution_staff,
            source_variable=f"{FIELD_STUDENTS_TOTAL} / {staff_basis}",
        )
        if row:
            rows.append(row)

        institution_ratio = row.ratio if row else None

        # Guard 1: is the staff breakdown complete enough to divide by?
        field_headcounts = {
            broad: _num(_get(record, _staff_hc_field(broad)))
            for broad in BROAD_FIELDS
        }
        reported = [v for v in field_headcounts.values() if v is not None]
        if not reported or not staff_hc_total or staff_hc_total <= 0:
            continue
        if sum(reported) / staff_hc_total < MIN_STAFF_BREAKDOWN_COVERAGE:
            dropped_incoherent += 1
            continue

        # ETER breaks staff down by field as headcount only, so convert to FTE
        # using the institution's own average FTE-per-head. Deliberately *not*
        # normalised by the sum of reported fields: that would hand the whole
        # institution's FTE to whichever fields happen to be reported.
        can_apportion = staff_fte_total is not None

        for broad in BROAD_FIELDS:
            students = _num(_get(record, _students_field(broad)))
            staff_hc = field_headcounts[broad]
            if students is None or staff_hc is None:
                continue

            if can_apportion:
                staff = (staff_hc / staff_hc_total) * staff_fte_total
                basis = (
                    f"({_staff_hc_field(broad)} / {FIELD_STAFF_HC_TOTAL})"
                    f" * {FIELD_STAFF_FTE_TOTAL}"
                )
            else:
                staff = staff_hc
                basis = _staff_hc_field(broad)

            field_row = _ratio_row(
                eter_id=eter_id,
                broad=broad,
                year=year,
                students=students,
                staff=staff,
                source_variable=f"{_students_field(broad)} / {basis}",
            )
            if not field_row:
                continue

            # Guard 2: does it plausibly refine the institution-wide figure?
            if institution_ratio and institution_ratio > 0:
                deviation = field_row.ratio / institution_ratio
                if not (1 / MAX_FIELD_DEVIATION <= deviation <= MAX_FIELD_DEVIATION):
                    dropped_implausible += 1
                    continue

            rows.append(field_row)

    logger.info(
        "Computed %s ratio row(s) for year %s "
        "(dropped %s institution(s) with an incoherent staff breakdown, "
        "%s field ratio(s) implausible vs the institution-wide figure)",
        len(rows), year, dropped_incoherent, dropped_implausible,
    )
    if stats is not None:
        stats.dropped_incoherent = dropped_incoherent
        stats.dropped_implausible = dropped_implausible
    return rows


# --------------------------------------------------------------------------
# Provider mapping
# --------------------------------------------------------------------------

def load_provider_uri_map(db: Session) -> Dict[str, str]:
    """Map ETER id -> provider IRI, in one query."""
    rows = db.execute(
        text("SELECT eter_id, base_id FROM provider WHERE eter_id IS NOT NULL")
    ).fetchall()
    out: Dict[str, str] = {}
    for eter_id, base_id in rows:
        if eter_id and base_id is not None:
            out[eter_id.strip()] = f"https://data.deqar.eu/institution/{base_id}"
    logger.info("Loaded %s ETER id -> provider IRI mapping(s)", len(out))
    return out


# --------------------------------------------------------------------------
# RDF
# --------------------------------------------------------------------------

def _ratio_iri(row: RatioRow) -> str:
    scope = "institution" if row.isced_f_broad is None else f"isced-f-{row.isced_f_broad}"
    return f"{STATS_IRI_BASE}{row.eter_id}/{scope}/{row.reference_year}"


def ratio_to_rdf(row: RatioRow, provider_uri: str) -> Tuple[str, str]:
    """Serialise one observation node. Returns (node_iri, n-triples)."""
    graph = Graph()
    graph.bind("ql", QL)
    graph.bind("dcterms", DCTERMS)

    node = URIRef(_ratio_iri(row))
    graph.add((node, RDF.type, QL.StudentStaffRatio))
    graph.add((node, QL.aboutOrganisation, URIRef(provider_uri)))
    graph.add((node, QL.ratioValue, Literal(row.ratio, datatype=XSD.double)))
    graph.add((node, QL.referenceYear, Literal(str(row.reference_year), datatype=XSD.gYear)))
    graph.add((node, DCTERMS.source, ETER_SOURCE_URI))
    if row.isced_f_broad is not None:
        graph.add((node, QL.ISCEDFBroadField, URIRef(f"{ISCED_F_BASE}{row.isced_f_broad}")))

    return str(node), graph.serialize(format="nt")


def purge_update(provider_uri: str, year: int) -> str:
    """SPARQL Update clearing one institution's ETER ratios for one year.

    Scoped to (institution, year, source) on purpose:

    - by institution rather than by ETER id, so a provider whose eter_id
      changed upstream still has its old nodes cleared;
    - by year, because each run covers one year — purging the institution
      outright would make `--year 2021` wipe the 2022 data;
    - by source, so a future non-ETER stats feed publishing the same class is
      left alone.
    """
    return f"""
PREFIX ql: <{QL}>
PREFIX dcterms: <{DCTERMS}>
PREFIX xsd: <{XSD}>

WITH <{GRAPH_STATS}>
DELETE {{ ?r ?p ?o }}
WHERE {{
  ?r a ql:StudentStaffRatio ;
     ql:aboutOrganisation <{provider_uri}> ;
     ql:referenceYear "{year}"^^xsd:gYear ;
     dcterms:source <{ETER_SOURCE_URI}> ;
     ?p ?o .
}}
"""


def push_ratios_to_fuseki(
    rows: List[RatioRow],
    provider_uri_map: Dict[str, str],
    *,
    year: int,
    stats: Optional[FetchStats] = None,
) -> FetchStats:
    """Purge then re-insert each matched institution's ratios in the stats graph."""
    stats = stats or FetchStats()

    by_eter_id: Dict[str, List[RatioRow]] = {}
    for row in rows:
        by_eter_id.setdefault(row.eter_id, []).append(row)

    with requests.Session() as session:
        for eter_id, institution_rows in by_eter_id.items():
            provider_uri = provider_uri_map.get(eter_id)
            if not provider_uri:
                stats.unmatched_ids.append(eter_id)
                continue
            stats.matched_ids += 1

            if not fuseki.sparql_update(
                purge_update(provider_uri, year),
                session=session,
                context=f"purging ETER ratios for <{provider_uri}> ({year})",
            ):
                stats.failed += len(institution_rows)
                continue

            for row in institution_rows:
                node_iri, nt = ratio_to_rdf(row, provider_uri)
                if fuseki.replace_subject_in_graph(
                    GRAPH_STATS, node_iri, nt, session=session
                ):
                    stats.pushed += 1
                    if row.isced_f_broad is None:
                        stats.institution_scope += 1
                    else:
                        stats.field_scope += 1
                else:
                    stats.failed += 1

    logger.info(
        "ETER push: pushed=%s failed=%s matched=%s unmatched=%s",
        stats.pushed, stats.failed, stats.matched_ids, len(stats.unmatched_ids),
    )
    return stats


def ratios_for_provider(provider_uri: str) -> List[Dict[str, Any]]:
    """Every stored ratio for one institution, all years and scopes."""
    query = f"""
PREFIX ql: <{QL}>
PREFIX dcterms: <{DCTERMS}>

SELECT ?ratio ?field ?value ?year ?source
FROM <{GRAPH_STATS}>
WHERE {{
  ?ratio a ql:StudentStaffRatio ;
         ql:aboutOrganisation <{provider_uri}> ;
         ql:ratioValue ?value ;
         ql:referenceYear ?year .
  OPTIONAL {{ ?ratio ql:ISCEDFBroadField ?field }}
  OPTIONAL {{ ?ratio dcterms:source ?source }}
}}
ORDER BY DESC(?year) ?field
"""
    out: List[Dict[str, Any]] = []
    for binding in fuseki.sparql_select(query):
        out.append({
            "uri": binding.get("ratio", {}).get("value"),
            "field": binding.get("field", {}).get("value"),
            "value": binding.get("value", {}).get("value"),
            "year": binding.get("year", {}).get("value"),
            "source": binding.get("source", {}).get("value"),
        })
    return out
