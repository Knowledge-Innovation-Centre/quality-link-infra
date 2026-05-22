import logging
import uuid
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from rdflib import BNode, Graph, Literal, Namespace, RDF, URIRef
from rdflib.namespace import DCTERMS, OWL, SKOS, XSD

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

    MODE_MAP = {
        "online":   URIRef("http://data.europa.eu/snb/learning-assessment/920fbb3cbe"), # Online
        "blended":  URIRef("http://data.europa.eu/snb/learning-assessment/c_3a90b26d"), # Hybrid
        "onGround": URIRef("http://data.europa.eu/snb/learning-assessment/9191af2ed9"), # Presential
    }

    COURSE_TYPE_MAP = {
        "internship": URIRef("http://data.europa.eu/snb/learning-opportunity/77b99de990"),
        "thesis":     URIRef("http://data.europa.eu/snb/learning-opportunity/b2434ca358"),
    }

    def _do_fetch(self, session):
        url = urljoin(self.source["path"], "courseTemplates")
        url_offerings = urljoin(self.source["path"], "courseOfferings")

        params = {}
        if self.source.get("parameters"):
            params.update(self.source["parameters"])
        params["limit"] = self.source.get("pageSize", 500)

        logger.info("Edu-API request to %s", url)

        params["offset"] = 0
        has_next_page = True

        courses = {}
        offerings = {}

        while has_next_page:
            response = session.get(url, params=params, timeout=60)
            response.raise_for_status()
            items = response.json()

            if len(items) >= params["limit"]:
                params["offset"] += params["limit"]
            else:
                has_next_page = False

            logger.info("Edu-API page: %s courses", len(items))

            for course in items:
                courses[course.get('sourcedId')] = course
                offerings[course.get('sourcedId')] = []

        logger.info("Edu-API request to %s", url_offerings)

        params["offset"] = 0
        has_next_page = True

        while has_next_page:
            response = session.get(url_offerings, params=params, timeout=60)
            response.raise_for_status()
            items = response.json()

            if len(items) >= params["limit"]:
                params["offset"] += params["limit"]
            else:
                has_next_page = False

            logger.info("Edu-API page: %s course offerings", len(items))

            for offering in items:
                if offering.get('course') in offerings:
                    offerings[offering.get('course')].append(offering)
                else:
                    logger.warning(f"- courseOffering {offering['sourcedId']} refers to unknown courseTemplate {offering['course']}")

        graph = Graph()
        graph.bind("ql", QL)
        graph.bind("elm", ELM)
        graph.bind("dcterms", DCTERMS)

        success_count = 0
        failed_count = 0

        for course_id in courses:
            if self.map_course_to_rdf(courses[course_id], graph, offerings[course_id]):
                success_count += 1
            else:
                failed_count += 1

        logger.info(
            "Edu-API fetch done: %s ok, %s failed, %s triples",
            success_count, failed_count, len(graph),
        )
        return graph.serialize(format="turtle", encoding="utf-8"), "text/turtle"

    def _org_uuid(self, obj: Dict) -> Optional[str]:
        organization = obj.get("organization")
        if not isinstance(organization, str) or not organization:
            return None
        try:
            return str(uuid.UUID(organization))
        except ValueError:
            logger.warning("Invalid organization UUID %r on %s", organization, obj.get("sourcedId"))
            return None

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

    def map_course_to_rdf(self, course: Dict, graph: Graph, offerings: List):
        courseId = course.get("sourcedId")
        if not courseId:
            return None

        course_uri = self._get_uri(courseId)
        course_uuid = self._get_uuid(courseId, course_uri)

        graph.add((course_uri, RDF.type, QL.LearningOpportunitySpecification))
        graph.add((course_uri, QL.sourceType, QL.EduApiSource))
        graph.add((URIRef(f"urn:uuid:{course_uuid}"), OWL.sameAs, course_uri))

        # type
        if "courseType" in course and course["courseType"] in self.COURSE_TYPE_MAP:
            graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE_MAP[course["courseType"]]))
        else:
            graph.add((course_uri, DCTERMS.type, self.COURSE_TYPE))

        # offering organisation
        org_uuid = self._org_uuid(course)
        if org_uuid:
            graph.add((course_uri, DCTERMS.publisher, URIRef(f"urn:uuid:{org_uuid}")))

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

        # try to parse credits
        if course.get("creditType") == "credit" and "creditsAwarded" in course:
            if m := re.match(r"\d+(\.\d+)?", course["creditsAwarded"]):
                credit = BNode()
                graph.add((credit, ELM.point, Literal(m[0], datatype=XSD.double)))
                if re.search("ECTS", course["creditsAwarded"], re.IGNORECASE):
                    graph.add((credit, ELM.framework, URIRef("http://data.europa.eu/snb/education-credit/6fcec5c5af")))
                graph.add((course_uri, ELM.creditPoint, credit))

        # Offerings of this course

        for offering in offerings:
            offeringId = offering.get("sourcedId")
            offering_uri = URIRef(
                f"{course_uri}/offerings/{offeringId}"
            )
            graph.add((offering_uri, RDF.type, QL.LearningOpportunityInstance))
            graph.add((offering_uri, ELM.learningAchievementSpecification, course_uri))

            offering_org_uuid = self._org_uuid(offering)
            if offering_org_uuid:
                graph.add((offering_uri, ELM.providedBy, URIRef(f"urn:uuid:{offering_org_uuid}")))

            if offering.get("primaryCode") and isinstance(offering["primaryCode"], dict):
                code = BNode()
                graph.add((code, RDF.type, ELM.Identifier))
                graph.add((code, SKOS.notation, Literal(offering["primaryCode"].get("identifier"))))
                graph.add((code, ELM.schemeName, Literal(offering["primaryCode"].get("identifierType"))))
                graph.add((offering_uri, ADMS.identifier, code))
            if offering.get("title"):
                title = self.extract_english_value(offering.get("title"))
                if title:
                    graph.add((offering_uri, DCTERMS.title, Literal(title, lang="en")))
            if offering.get("description"):
                description = self.extract_english_value(offering.get("description"))
                if description:
                    graph.add((offering_uri, DCTERMS.description, Literal(description, lang="en")))

            """
            if offering.get("teachingLanguage"):
                lang_code = offering.get("teachingLanguage")
                if isinstance(lang_code, str):
                    graph.add((offering_uri, DCTERMS.language, URIRef(
                        f"http://publications.europa.eu/resource/authority/language/{lang_code.upper()}"
                    )))
            """

            if offering.get("startDate") or offering.get("endDate") or offering.get("academicSessionCode"):
                temporal = BNode()
                graph.add((temporal, RDF.type, DCTERMS.PeriodOfTime))
                if offering.get("startDate"):
                    graph.add((temporal, ELM.startDate, Literal(offering.get("startDate"), datatype=XSD.date)))
                if offering.get("endDate"):
                    graph.add((temporal, ELM.endDate, Literal(offering.get("endDate"), datatype=XSD.date)))
                if offering.get("academicSessionCode"):
                    graph.add((temporal, SKOS.prefLabel, Literal(offering.get("academicSessionCode"))))
                graph.add((offering_uri, DCTERMS.temporal, temporal))

            self._value_to_concept(offering, "offeringFormat", graph, offering_uri, ELM.mode, self.MODE_MAP)

            self._value_to_literal(offering, "maxNumberStudents",       graph, offering_uri, QL.enrolmentCapacity,      datatype=XSD.nonNegativeInteger)
            self._value_to_literal(offering, "enrolledNumberStudents",  graph, offering_uri, QL.enrolledLearnerCount,   datatype=XSD.nonNegativeInteger)
            self._value_to_literal(offering, "minNumberStudents",       graph, offering_uri, QL.enrolmentMinimum,       datatype=XSD.nonNegativeInteger)

        return course_uuid
