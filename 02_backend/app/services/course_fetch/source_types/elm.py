import logging
import os
from urllib.parse import urlparse

from .base import DataSourceType

logger = logging.getLogger(__name__)


class ElmDataSource(DataSourceType):
    def _do_fetch(self, session):
        logger.info("Downloading ELM file from %s", self.source["path"])

        response = session.get(self.source["path"], timeout=60)
        response.raise_for_status()

        file_extension = os.path.splitext(urlparse(self.source["path"]).path)[1]
        content_type = response.headers.get("content-type")

        if "contentType" in self.source and self.source["contentType"] in self.OK_TYPES:
            if self.source["contentType"] != content_type:
                logger.warning(
                    "Source config overrides actual content-type %r → %r",
                    content_type, self.source["contentType"],
                )
            return response.content, self.source["contentType"]

        if content_type not in self.OK_TYPES:
            logger.warning(
                "Unsupported content-type %r; guessing from extension %r",
                content_type, file_extension,
            )
            if file_extension in (".rdf", ".xml"):
                content_type = "application/rdf+xml"
            elif file_extension in (".json", ".jsonld"):
                content_type = "application/ld+json"
            elif file_extension == ".ttl":
                content_type = "text/turtle"

        return response.content, content_type
