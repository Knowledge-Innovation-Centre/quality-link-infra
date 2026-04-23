import logging
import uuid
from typing import Any, Dict
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, SKOS, XSD

from .base import DataSourceType

logger = logging.getLogger(__name__)

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")
ADMS = Namespace("http://www.w3.org/ns/adms#")


class OoapiDataSource(DataSourceType):
    """OOAPI (v5) data source."""

    LEVEL_MAP = {
        "secondary vocational education 1": URIRef("http://data.europa.eu/snb/eqf/1"),
        "secondary vocational education 2": URIRef("http://data.europa.eu/snb/eqf/2"),
        "secondary vocational education 3": URIRef("http://data.europa.eu/snb/eqf/3"),
        "secondary vocational education 4": URIRef("http://data.europa.eu/snb/eqf/4"),
        "associate degree": URIRef("http://data.europa.eu/snb/eqf/5"),
        "bachelor": URIRef("http://data.europa.eu/snb/eqf/6"),
        "master": URIRef("http://data.europa.eu/snb/eqf/7"),
        "doctoral": URIRef("http://data.europa.eu/snb/eqf/8"),
    }

    def _do_fetch(self, session):
        url = urljoin(self.source["path"], "courses")
        logger.info("OOAPI request to %s", url)

        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])
        params["pageSize"] = self.source.get("pageSize", 250)
        params["pageNumber"] = 0

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        success_count = 0
        failed_count = 0
        has_next_page = True

        while has_next_page:
            params["pageNumber"] += 1
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            items = data.get("items", [])
            page = data.get("pageNumber", 1)
            has_next_page = data.get("hasNextPage", False)

            logger.info("OOAPI page %s: %s courses", page, len(items))

            for course in items:
                if self.map_course_to_rdf(course, graph):
                    success_count += 1
                else:
                    failed_count += 1

        logger.info(
            "OOAPI fetch done: %s ok, %s failed, %s triples",
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
        courseId = course.get("courseId")
        if not courseId:
            return None

        course_uri = URIRef(
            f"http://data.quality-link.eu/providers/{self.source['provider_uuid']}/courses/{courseId}"
        )
        course_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, str(course_uri)))

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE))
        graph.add((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, course_uri))

        if course.get("primaryCode") and isinstance(course["primaryCode"], dict):
            code = BNode()
            graph.add((code, RDF.type, ELM.Identifier))
            graph.add((code, SKOS.notation, Literal(course["primaryCode"].get("code"))))
            graph.add((code, ELM.schemeName, Literal(course["primaryCode"].get("codeType"))))
            graph.add((course_uri, ADMS.identifier, code))

        if course.get("abbreviation"):
            abbreviation = BNode()
            graph.add((abbreviation, RDF.type, ELM.Identifier))
            graph.add((abbreviation, SKOS.notation, Literal(course["abbreviation"])))
            graph.add((course_uri, ADMS.identifier, abbreviation))

        if course.get("name"):
            title = self.extract_english_value(course.get("name"))
            if title:
                graph.add((course_uri, DCTERMS.title, Literal(title, lang="en")))

        if course.get("description"):
            description = self.extract_english_value(course.get("description"))
            if description:
                graph.add((course_uri, DCTERMS.description, Literal(description, lang="en")))

        if course.get("learningOutcomes"):
            outcomes = [
                v for outcome in course["learningOutcomes"]
                if (v := self.extract_english_value(outcome))
            ]
            if outcomes:
                losummary = BNode()
                graph.add((losummary, RDF.type, ELM.Note))
                for outcome in outcomes:
                    graph.add((losummary, ELM.noteLiteral, Literal(outcome, lang="en")))
                graph.add((course_uri, ELM.learningOutcomeSummary, losummary))

        study_load = course.get("studyLoad")
        if isinstance(study_load, dict) and study_load.get("value"):
            if study_load.get("studyLoadUnit", "ects") == "ects":
                ects = BNode()
                graph.add((ects, ELM.point, Literal(study_load["value"], datatype=XSD.double)))
                graph.add((ects, ELM.framework, URIRef("http://data.europa.eu/snb/education-credit/6fcec5c5af")))
                graph.add((course_uri, ELM.creditPoint, ects))
            else:
                graph.add((course_uri, ELM.volumeOfLearning, Literal(study_load["value"], datatype=XSD.decimal)))

        if course.get("level") and course["level"] in self.LEVEL_MAP:
            graph.add((course_uri, ELM.EQFLevel, self.LEVEL_MAP[course["level"]]))

        if course.get("teachingLanguage"):
            lang_code = course.get("teachingLanguage")
            if isinstance(lang_code, str):
                graph.add((course_uri, DCTERMS.language, URIRef(
                    f"http://publications.europa.eu/resource/authority/language/{lang_code.upper()}"
                )))

        if fields := course.get("fieldsOfStudy"):
            if isinstance(fields, list):
                for field in fields:
                    if isinstance(field, str):
                        graph.add((course_uri, ELM.ISCEDFCode, URIRef(f"http://data.europa.eu/snb/isced-f/{field}")))
            elif isinstance(fields, str):
                graph.add((course_uri, ELM.ISCEDFCode, URIRef(f"http://data.europa.eu/snb/isced-f/{fields}")))

        if course.get("link"):
            web = BNode()
            graph.add((web, RDF.type, ELM.WebResource))
            graph.add((web, ELM.contentUrl, Literal(course.get("link"))))
            graph.add((course_uri, FOAF.homepage, web))

        return course_uuid
