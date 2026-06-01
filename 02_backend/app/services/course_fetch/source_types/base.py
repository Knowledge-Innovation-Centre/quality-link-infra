import logging
import re
import uuid
from typing import Any, Dict, Optional

import requests
from rdflib import BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import RDF, SKOS, XSD

logger = logging.getLogger(__name__)

ELM = Namespace("http://data.europa.eu/snb/model/elm/")
ADMS = Namespace("http://www.w3.org/ns/adms#")


# ---------------------------------------------------------------------------
# EU vocabulary URI stubs
#
# Each function turns a simple code into the URI used by an EU controlled
# vocabulary. Kept as named helpers so the base path or behaviour can be
# changed in one place (e.g. swap to a lookup against a cached vocab).
# ---------------------------------------------------------------------------

_DIGITS_2_6_RE = re.compile(r"^\d{2,6}$")


def _normalize_isced(code: str) -> Optional[str]:
    """Return the 2–4 digit ISCED-F 2013 form of `code`.

    SOI2021 (Dutch) extensions are 4-digit ISCED-F + 2 narrowing digits,
    so 5–6 digit codes are truncated to their 4-digit ISCED-F parent.
    Returns None if the input is not 2–6 all-digits.
    """
    if not isinstance(code, str) or not _DIGITS_2_6_RE.match(code):
        return None
    return code[:4]


def isced_f_code_to_uri(code: str) -> Optional[URIRef]:
    """Map an ISCED-F 2013 (or SOI2021-extended) code to its EU snb URI."""
    base = _normalize_isced(code)
    if base is None:
        return None
    return URIRef(f"http://data.europa.eu/snb/isced-f/{base}")


def country_code_to_uri(code: str) -> Optional[URIRef]:
    """Map an ISO 3166 alpha-2 or alpha-3 country code to its EU authority URI."""
    if not isinstance(code, str) or not code:
        return None
    return URIRef(f"http://publications.europa.eu/resource/authority/country/{code.upper()}")


def currency_code_to_uri(code: str) -> Optional[URIRef]:
    """Map an ISO 4217 currency code to its EU authority URI."""
    if not isinstance(code, str) or not code:
        return None
    return URIRef(f"http://publications.europa.eu/resource/authority/currency/{code.upper()}")


def org_uuid_from_value(value, owner: Optional[str] = None) -> Optional[str]:
    """Return a normalized UUID string from a value that should already be a UUID.

    `owner` is an optional identifier of the carrying object, used only in the
    warning log when the value is not a UUID.
    """
    if not isinstance(value, str) or not value:
        return None
    try:
        return str(uuid.UUID(value))
    except ValueError:
        logger.warning("Invalid organization UUID %r on %s", value, owner)
        return None


def get_date_datatype(value: str):
    """Pick XSD.dateTime vs XSD.date based on whether `value` is RFC3339 date-time."""
    if isinstance(value, str) and "T" in value:
        return XSD.dateTime
    return XSD.date


class DataSourceType:
    """Base class for data sources.

    Subclass per source type (ELM, OOAPI, Edu-API, ...) and implement _do_fetch().
    fetch() wraps session lifecycle and header defaults.
    """

    OK_TYPES = (
        "application/rdf+xml",
        "application/xml",
        "text/xml",
        "text/turtle",
        "application/json",
        "application/ld+json",
    )

    COURSE_TYPE = URIRef("http://data.europa.eu/snb/learning-opportunity/05053c1cbe")
    PROGRAMME_TYPE = URIRef("http://data.europa.eu/snb/learning-opportunity/79343569f3")

    def __init__(self, source: Dict):
        self.source = source
        self._headers = {"user-agent": "quality-link-aggregator/1.0.0-alpha"}
        auth = source.get("auth") or {}
        if auth.get("type") == "httpheader":
            self._headers[auth.get("field", "x-qualitylink-auth")] = auth.get("value")
        if source.get("headers"):
            self._headers.update(source["headers"])

    def fetch(self):
        """Opens a session, delegates to _do_fetch(), closes session on exit.

        Returns (content bytes, MIME content-type).
        """
        with requests.Session() as session:
            auth = self.source.get("auth") or {}
            if auth.get("type") == "oauth2.0":
                # Deferred to fetch() because it needs a DB session to look up
                # out-of-band credentials. Imports kept local to avoid pulling
                # SQLAlchemy into module-import time for source-type modules.
                from database import SessionLocal
                from services.oauth import get_oauth_token

                with SessionLocal() as db:
                    token = get_oauth_token(db, self.source["provider_uuid"], auth)
                session.headers["Authorization"] = f"Bearer {token}"
            session.headers.update(self._headers)
            return self._do_fetch(session)


    def _do_fetch(self, session):
        raise NotImplementedError


    def _get_uri(self, source_id):
        """
        Return a URI composed of provider identifier and unique identifier provided by the source
        """
        return URIRef(
            f"http://data.quality-link.eu/courses/{self.source['provider_id']}/{source_id}"
        )


    def _get_uuid(self, source_id, uri):
        """
        If source_id is a valid UUID, return this - otherwise generate UUID from URI
        """
        try:
            return uuid.UUID(source_id)
        except ValueError:
            return uuid.uuid5(uuid.NAMESPACE_URL, str(uri))


    def extract_english_value(self, multilingual_field: Any) -> str:
        """Pull the English value out of a multilingual field.

        Accepts a plain string, or a list of `{language, value}` dicts (the
        shape both OOAPI and Edu-API use). Falls back to the first entry's
        value if no English-tagged entry is found.
        """
        if isinstance(multilingual_field, str):
            return multilingual_field
        if not isinstance(multilingual_field, list) or not multilingual_field:
            return ""
        for item in multilingual_field:
            if isinstance(item, dict):
                lang = item.get("language", "").lower()
                if "en" in lang:
                    return item.get("value", "")
        if isinstance(multilingual_field[0], dict):
            return multilingual_field[0].get("value", "")
        return str(multilingual_field[0])


    def _value_to_literal(self, source_dict, key, graph, subject, predicate, datatype = None, lang = None):
        """
        Add value to RDF graph as literal, if exists
        """
        if source_dict.get(key):
            graph.add((subject, predicate, Literal(source_dict[key], datatype=datatype, lang=lang)))


    def _value_to_concept(self, source_dict, key, graph, subject, predicate, mapping):
        """
        Add value to RDF graph based on concept mapping
        """
        if source_dict.get(key) and source_dict[key] in mapping:
            graph.add((subject, predicate, mapping[source_dict[key]]))


    def _add_identifier(
        self,
        graph: Graph,
        subject: URIRef,
        code,
        scheme_name=None,
        scheme_id: Optional[URIRef] = None,
    ) -> None:
        """Attach an adms:identifier elm:Identifier blank node to `subject`."""
        if code is None or code == "":
            return
        ident = BNode()
        graph.add((ident, RDF.type, ELM.Identifier))
        graph.add((ident, SKOS.notation, Literal(str(code))))
        if scheme_name:
            graph.add((ident, ELM.schemeName, Literal(str(scheme_name))))
        if scheme_id is not None:
            graph.add((ident, ELM.schemeId, scheme_id))
        graph.add((subject, ADMS.identifier, ident))
