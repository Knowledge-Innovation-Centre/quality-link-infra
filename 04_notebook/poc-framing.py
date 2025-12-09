#!/usr/bin/env python
# coding: utf-8

# =============================================================================
# RDF TO MEILISEARCH PIPELINE
# =============================================================================

# =============================================================================
# SECTION 1: SETUP AND DEPENDENCIES
# =============================================================================

import argparse
import pathlib
from rdflib import Graph
from pyld import jsonld
import requests
import json
import sys
import os
import json
import uuid
import re
import time
import meilisearch
import uuid

print("✅ Dependencies loaded successfully!")


# =============================================================================
# SECTION 2: CONFIGURATION
# =============================================================================

parser = argparse.ArgumentParser(description='n!=??!J!', epilog="this is da epilog")
parser.add_argument('URI', nargs='?', help='fetch this specific learning opportunity')
parser.add_argument('-f', '--frame', type=pathlib.Path, help='JSON-LD frame document')
parser.add_argument('-d', '--dump', action='store_true', help='dump JSON-LD before upload')
args = parser.parse_args()

# Jena Fuseki Configuration
FUSEKI_URL = os.environ.get('FUSEKI_URL', "https://fuseki.app.quality-link.eu")
DATASET_NAME = os.environ.get('FUSEKI_DATASET', "pipeline-data")
FUSEKI_USERNAME = os.environ.get('FUSEKI_USERNAME', 'admin')
FUSEKI_PASSWORD = os.environ.get('FUSEKI_PASSWORD')

# Meilisearch Configuration  
MEILISEARCH_URL = os.environ.get('MEILISEARCH_URL', "https://lwowo04cs888sswsswoc4kwo.serverfarm.knowledgeinnovation.eu")
MEILISEARCH_API_KEY = os.environ.get('MEILISEARCH_API_KEY')
INDEX_NAME = os.environ.get('MEILISEARCH_INDEX', "test-index")

print("⚙️ Configuration variables set")

# Set up authentication if needed
auth = None
if FUSEKI_USERNAME and FUSEKI_PASSWORD:
    auth = (FUSEKI_USERNAME, FUSEKI_PASSWORD)

# Build query URL
query_url = f"{FUSEKI_URL}/{DATASET_NAME}/sparql"

# =============================================================================
# SECTION 3: GET LIST OF LEARNING OPPORTUNITIES
# =============================================================================

# Construct SPARQL query to get all LOs
query_los_list = """
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX ql: <http://data.quality-link.eu/ontology/v1#>
PREFIX elm: <http://data.europa.eu/snb/model/elm/>

SELECT ?learningOpportunity
WHERE {
  {
    ?learningOpportunity rdf:type ql:LearningOpportunitySpecification .
  } UNION {
    ?learningOpportunity rdf:type elm:LearningAchievementSpecification .
  } UNION {
    ?learningOpportunity rdf:type elm:Qualification .
  }
}
LIMIT 100
"""

# Execute LOS query  
try:
    los_response = requests.get(query_url, params={'query': query_los_list, 'format': 'application/sparql-results+json'}, auth=auth, timeout=15)
    los_response.raise_for_status()
    los_results = los_response.json()['results']['bindings'] if los_response.status_code == 200 else []
    print(f"✅ LOS query: {len(los_results)} results")
except Exception as e:
    print(f"❌ LOS query failed: {e}")
    raise e

# =============================================================================
# SECTION 4: DATA TRANSFORMATION FOR MEILISEARCH
# =============================================================================

# Helper functions for data processing
def clean_id(uri):
    """Clean URI to make it Meilisearch-compatible ID"""
    clean = re.sub(r'[^a-zA-Z0-9\-_]', '_', uri)
    return re.sub(r'_+', '_', clean).strip('_')

def extract_value(binding, key):
    """Extract value from SPARQL binding result"""
    if key in binding:
        return binding[key]['value']
    return None

def extract_language_value(binding, key):
    """Extract value with language info from SPARQL binding"""
    if key in binding:
        value = binding[key]['value']
        lang = binding[key].get('xml:lang', 'en')
        return {'value': value, 'language': lang}
    return None

# Set up headers for Meilisearch requests
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {MEILISEARCH_API_KEY}"
}

# Create the index
create_url = f"{MEILISEARCH_URL}/indexes"
index_data = {
    "uid": INDEX_NAME,
    "primaryKey": "id"
}

print(f"🏗️ Creating index...")

create_response = requests.post(create_url, headers=headers, json=index_data)
print(f"📋 Create index response: {create_response.status_code}")

if create_response.status_code not in [200, 201, 202]:
    print(f"⚠️  Index might already exist or there was an error: {create_response.text}")

# Build Learning Opportunity Specification documents  
print("📚 Processing Learning Opportunity Specifications...")

with open(args.frame) as f:
    lo_frame =  json.load(f)

# Upload the documents
upload_url = f"{MEILISEARCH_URL}/indexes/{INDEX_NAME}/documents"

for result in los_results:
    lo_uri = extract_value(result, 'learningOpportunity')
    print(f"###\n### {lo_uri}\n###\n")

    query_lo_single = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

        CONSTRUCT {{
          ?s ?p ?o .
        }}
        WHERE {{
          <{lo_uri}> (<>|!<>)* ?s .
          ?s ?p ?o .
        }}
        """
    lo_response = requests.get(query_url, params={'query': query_lo_single, 'format': 'application/ld+json'}, auth=auth, timeout=15)
    lo_response.raise_for_status()

    # use JSON-LD framing
    framed_json = jsonld.frame(lo_response.json(), lo_frame)
    if '@context' in framed_json:
        del framed_json['@context'] # drop context for Meilisearch
    framed_json['id'] = str(uuid.uuid5(uuid.NAMESPACE_URL, lo_uri)) # use UUIDv5, suitable for Meilisearch

    if args.dump:
        json.dump(framed_json, sys.stdout, indent=4)

    upload_response = requests.post(
        upload_url,
        headers=headers,
        json=framed_json
    )

    print(f"📋 Upload response: {upload_response.status_code}")

    if upload_response.status_code == 202:
        task_info = upload_response.json()
        task_uid = task_info['taskUid']
        print(f"✅ Documents uploaded! Task UID: {task_uid}")

        # Monitor the indexing task
        print("⏳ Monitoring indexing progress...")
        task_url = f"{MEILISEARCH_URL}/tasks/{task_uid}"

        for i in range(15):  # Check up to 15 times
            response = requests.get(task_url, headers={"Authorization": f"Bearer {MEILISEARCH_API_KEY}"})

            if response.status_code == 200:
                task_data = response.json()
                status = task_data['status']
                print(f"📋 Task status: {status}")

                if status == 'succeeded':
                    print("🎉 SUCCESS! Documents indexed successfully!")
                    break
                elif status == 'failed':
                    print("❌ Indexing failed!")
                    print(f"Error: {task_data.get('error', {})}")
                    break
                else:
                    time.sleep(2)  # Wait 2 seconds before checking again
            else:
                print(f"❌ Error checking task: {response.status_code}")
                break
    else:
        print(f"❌ Upload failed: {upload_response.text}")

