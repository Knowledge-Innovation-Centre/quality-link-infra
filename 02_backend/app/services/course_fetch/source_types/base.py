from typing import Dict

import uuid

import requests
from rdflib import URIRef, Literal


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


