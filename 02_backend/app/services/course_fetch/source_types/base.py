from typing import Dict

import requests
from rdflib import URIRef


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
        if source.get("auth") and source["auth"].get("type") == "httpheader":
            self._headers[source["auth"].get("field", "x-qualitylink-auth")] = source["auth"].get("value")
        if source.get("headers"):
            self._headers.update(source["headers"])

    def fetch(self):
        """Opens a session, delegates to _do_fetch(), closes session on exit.

        Returns (content bytes, MIME content-type).
        """
        with requests.Session() as session:
            session.headers.update(self._headers)
            return self._do_fetch(session)

    def _do_fetch(self, session):
        raise NotImplementedError
