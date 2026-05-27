import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "postgres")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "backend")
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

SERVICE_URL_FRONTEND = os.getenv("SERVICE_URL_FRONTEND", "https://dashboard.app.quality-link.eu")

FUSEKI_URL = os.getenv("FUSEKI_URL", "http://fuseki:3030")
FUSEKI_USERNAME = os.getenv("FUSEKI_USERNAME")
FUSEKI_PASSWORD = os.getenv("FUSEKI_PASSWORD")
FUSEKI_DATASET_NAME = os.getenv("FUSEKI_DATASET_NAME")

MEILISEARCH_URL = os.getenv("MEILISEARCH_URL", "http://meilisearch:7700")
MEILISEARCH_API_KEY = os.getenv("MEILISEARCH_API_KEY")
MEILISEARCH_INDEX = os.getenv("MEILISEARCH_INDEX")

DEQAR_API_URL = os.getenv(
    "DEQAR_API_URL", "https://backend.testzone.eqar.eu/connectapi/v1/providers/"
)

GRAPH_COURSES = "http://data.quality-link.eu/graph/courses"
GRAPH_REFERENCE = "http://data.quality-link.eu/graph/reference"
GRAPH_VOCABULARY = "http://data.quality-link.eu/graph/vocabulary"

DEFAULT_VOCABULARIES = [
    # Each entry: {"scheme": <uri>, "properties": [<extra prop uri>, ...]}.
    # "properties" is optional; if omitted, only skos:prefLabel is fetched.
    {   "scheme": "http://data.europa.eu/snb/isced-f/25831c2" },                  # ISCED Fields of Study
    {   "scheme": "http://publications.europa.eu/resource/authority/language",    # Languages
        "properties": [
            "http://www.w3.org/2004/02/skos/core#notation",
        ],
    },
    {   "scheme": "http://data.europa.eu/snb/eqf/25831c2" },                      # EQF levels
    {   "scheme": "http://data.europa.eu/snb/learning-opportunity/25831c2" },     # Learning opportunity type
    {   "scheme": "http://data.europa.eu/snb/learning-assessment/25831c2" },      # Mode of learning and assessment
    {   "scheme": "http://data.europa.eu/snb/learning-schedule/25831c2" },        # Learning schedule (part-time/full-time)
    {   "scheme": "http://publications.europa.eu/resource/authority/country" },   # Countries and territories
]

SCHEMA_DIR = Path(os.getenv("SCHEMA_DIR", Path(__file__).resolve().parent / "schema"))
