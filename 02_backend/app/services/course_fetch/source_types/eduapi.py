import logging
import uuid
from typing import Any, Dict
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, OWL, SKOS

from .base import DataSourceType

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")
ADMS = Namespace("http://www.w3.org/ns/adms#")


class EduApiDataSource(DataSourceType):
    """Edu-API (v1) data source."""

    LEVEL_MAP = {
        "undergraduate": URIRef("http://data.europa.eu/snb/eqf/6"),
        "graduate": URIRef("http://data.europa.eu/snb/eqf/7"),
        "doctoral": URIRef("http://data.europa.eu/snb/eqf/8"),
    }

    def _do_fetch(self, session):
        url = urljoin(self.source["path"], "courseTemplates")
        logger.info("Edu-API request to %s", url)

        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        response = session.get(url, params=params, timeout=60)
        response.raise_for_status()
        items = response.json()

        logger.info("Edu-API page: %s courses", len(items))

        success_count = 0
        failed_count = 0
        for course in items:
            if self.map_course_to_rdf(course, graph):
                success_count += 1
            else:
                failed_count += 1

        logger.info(
            "Edu-API fetch done: %s ok, %s failed, %s triples",
            success_count, failed_count, len(graph),
        )
        return graph.serialize(format="turtle", encoding="utf-8"), "text/turtle"

    def extract_english_value(self, multilingual_field: Any) -> str:
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

    def map_course_to_rdf(self, course: Dict, graph: Graph):
        courseId = course.get("sourcedId")
        if not courseId:
            return None

        course_uri = self._get_uri(courseId)
        course_uuid = self._get_uuid(courseId, course_uri)

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE))
        graph.add((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, course_uri))

        if course.get("primaryCode") and isinstance(course["primaryCode"], dict):
            code = BNode()
            graph.add((code, RDF.type, ELM.Identifier))
            graph.add((code, SKOS.notation, Literal(course["primaryCode"].get("identifier"))))
            graph.add((code, ELM.schemeName, Literal(course["primaryCode"].get("identifierType"))))
            graph.add((course_uri, ADMS.identifier, code))

        if course.get("title"):
            title = self.extract_english_value(course.get("title"))
            if title:
                graph.add((course_uri, DCTERMS.title, Literal(title, lang="en")))

        if course.get("description"):
            description = self.extract_english_value(course.get("description"))
            if description:
                graph.add((course_uri, DCTERMS.description, Literal(description, lang="en")))

        if course.get("level") and course["level"] in self.LEVEL_MAP:
            graph.add((course_uri, ELM.EQFLevel, self.LEVEL_MAP[course["level"]]))

        return course_uuid
