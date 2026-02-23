from typing import Optional, Tuple, List, Dict, Any
from urllib.parse import urlparse
from base64 import b64decode

from fastapi import HTTPException, status

from sqlalchemy import text
from sqlalchemy.orm import Session

from Crypto.PublicKey import RSA
from Crypto.Cipher import PKCS1_OAEP

import dns.resolver
import requests
import yaml
import json
import uuid as uuid_lib
from datetime import date

def get_private_key(db: Session) -> Optional[str]:
    result = db.execute(
        text("SELECT private_key FROM ql_cred WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1")
    ).fetchone()
    return result[0] if result else None


def decrypt_auth_value(encrypted_b64: str, private_key_pem: str) -> str:
    key = RSA.import_key(private_key_pem)
    cipher = PKCS1_OAEP.new(key)
    return cipher.decrypt(b64decode(encrypted_b64)).decode("utf-8")


def pull_manifest(provider_uuid: str, metadata: Dict, db: Session) -> Dict[str, Any]:
    """
    Pulls a manifest file for the given provider
    """

    identifiers = metadata.get("identifiers", [])
    schac_identifier = next(
        (item["identifier"] for item in identifiers if item.get("resource") == "SCHAC"),
        None,
    )
    website_link = metadata.get("website_link")

    if not schac_identifier and not website_link:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Neither SCHAC identifier nor website found in metadata",
        )

    test_combinations = prepare_test_combinations(schac_identifier, website_link)

    manifest_found = False
    manifest_url = None
    manifest_data = None

    for i, test in enumerate(test_combinations):
        if test["domain"] is None or manifest_found:
            test["check"] = None
            continue

        if test["type"] == "DNS":
            manifest_path = get_txt_records(test["domain"])
            if manifest_path:
                manifest_url, manifest_data = validate_manifest_url(manifest_path)
                if manifest_url:
                    manifest_found = True
                    test["check"] = True
                    test["path"] = manifest_url
                    for j in range(i + 1, len(test_combinations)):
                        test_combinations[j]["check"] = None
                else:
                    test["check"] = False
            else:
                test["check"] = False

        elif test["type"] == ".well-known":
            manifest_url, manifest_data = check_well_known(test["domain"])
            if manifest_url and manifest_data:
                manifest_found = True
                test["check"] = True
                test["path"] = manifest_url
                for j in range(i + 1, len(test_combinations)):
                    test_combinations[j]["check"] = None
            else:
                test["check"] = False

    db.execute(
        text("""
            UPDATE provider
            SET manifest_json = CAST(:manifest_json AS jsonb),
                last_manifest_pull = NOW()
            WHERE provider_uuid = :provider_uuid
        """),
        {"manifest_json": json.dumps(test_combinations), "provider_uuid": str(provider_uuid)},
    )

    sources_processed = False
    new_source_version_created = False

    if manifest_found and manifest_data and isinstance(manifest_data, dict) and "sources" in manifest_data:
        sources = manifest_data["sources"]
        if sources:
            sources_processed = True

            latest_version = db.execute(
                text("""
                    SELECT source_version_uuid, version_date, version_id, source_json
                    FROM source_version
                    WHERE provider_uuid = :provider_uuid
                    ORDER BY version_date DESC, version_id DESC
                    LIMIT 1
                """),
                {"provider_uuid": provider_uuid},
            ).fetchone()

            if not (latest_version and json.dumps(latest_version[3], sort_keys=True) == json.dumps(sources, sort_keys=True)):

                today = date.today().isoformat()
                version_id = 1
                if latest_version and latest_version[1].isoformat() == today:
                    version_id = latest_version[2] + 1

                source_uuid_json = [
                    {**source, "source_uuid": str(uuid_lib.uuid4())}
                    for source in sources
                ]

                new_version_result = db.execute(
                    text("""
                        INSERT INTO source_version
                        (provider_uuid, version_date, version_id, source_json, source_uuid_json)
                        VALUES
                        (:provider_uuid, :version_date, :version_id,
                         CAST(:source_json AS jsonb), CAST(:source_uuid_json AS jsonb))
                        RETURNING source_version_uuid
                    """),
                    {
                        "provider_uuid": provider_uuid,
                        "version_date": today,
                        "version_id": version_id,
                        "source_json": json.dumps(sources),
                        "source_uuid_json": json.dumps(source_uuid_json),
                    },
                ).fetchone()

                source_version_uuid = new_version_result[0]

                source_records = []
                for item in source_uuid_json:
                    if not (item.get("source_uuid") and item.get("path") and item.get("type")):
                        continue
                    source_auth = item.pop("auth", None)
                    if isinstance(source_auth, dict) and source_auth.get("type") == "httpheader":
                        encrypted_value = source_auth.get("value")
                        if encrypted_value:
                            try:
                                decrypted = decrypt_auth_value(encrypted_value, get_private_key(db))
                                source_auth["value"] = decrypted
                            except Exception:
                                print(f"Failed to decrypt auth for source {item.get('source_uuid')}")
                    source_records.append({
                        "source_uuid": item.pop("source_uuid"),
                        "source_version_uuid": source_version_uuid,
                        "source_id": item.pop("id", None),
                        "source_path": item.pop("path"),
                        "source_type": item.pop("type"),
                        "source_version": item.pop("version", None),
                        "source_name": item.pop("name", ""),
                        "source_refresh": int(item.pop("refresh", 0)),
                        "source_auth": json.dumps(source_auth) if source_auth else None,
                        "source_headers": json.dumps(item.pop("headers")) if "headers" in item else None,
                        "source_parameters": json.dumps(item.pop("queryParameters")) if "queryParameters" in item else None,
                        "source_other": json.dumps(item) if item else None,
                    })

                if source_records:
                    db.execute(
                        text("""
                            INSERT INTO source
                            (source_uuid, source_version_uuid, source_id, source_path, source_type, source_version, source_name, source_refresh, source_auth, source_headers, source_parameters, source_other)
                            VALUES
                            (:source_uuid, :source_version_uuid, :source_id, :source_path, :source_type, :source_version, :source_name, :source_refresh, CAST(:source_auth AS jsonb), CAST(:source_headers AS jsonb), CAST(:source_parameters AS jsonb), CAST(:source_other AS jsonb) )
                        """),
                        source_records,
                    )

                new_source_version_created = True

    db.commit()

    return {
        "status": "success",
        "provider_uuid": str(provider_uuid),
        "schac_domain": schac_identifier,
        "website_link": website_link,
        "manifest_url": manifest_url,
        "manifest_found": manifest_found,
        "manifest_json": test_combinations,
        "sources_processed": sources_processed,
        "new_source_version_created": new_source_version_created,
    }

def get_txt_records(domain: str) -> Optional[str]:
    if not domain:
        return None
    try:
        answers = dns.resolver.resolve(domain, 'TXT')
        for rdata in answers:
            txt_record = ''.join(
                rdata.strings[0].decode() if isinstance(rdata.strings[0], bytes) else rdata.strings[0]
            )
            if "m=" in txt_record:
                return txt_record.split("m=")[-1].strip()
        return None
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN, Exception):
        return None


def check_well_known(domain: str) -> Tuple[Optional[str], Optional[dict]]:
    if not domain:
        return None, None

    base_url = f"https://{domain}"
    well_known_paths = [
        "/.well-known/quality-link-manifest",
        "/.well-known/quality-link-manifest.json",
        "/.well-known/quality-link-manifest.yaml",
    ]

    for path in well_known_paths:
        full_url = f"{base_url.rstrip('/')}{path}"
        try:
            response = requests.get(full_url, timeout=30)
            if response.status_code == 200:
                content_type = response.headers.get("content-type", "")
                if content_type.startswith("application/json") or path.endswith(".json"):
                    try:
                        return full_url, response.json()
                    except Exception:
                        pass
                if (
                    content_type.startswith("application/yaml")
                    or content_type.startswith("application/x-yaml")
                    or path.endswith(".yaml")
                ):
                    try:
                        return full_url, yaml.safe_load(response.text)
                    except Exception:
                        pass
                if response.text and len(response.text.strip()) > 0:
                    return full_url, {"raw_path": True, "content_type": content_type}
        except Exception:
            continue

    return None, None


def validate_manifest_url(url: str) -> Tuple[Optional[str], Optional[dict]]:
    if not url:
        return None, None
    try:
        manifest_resp = requests.get(url, timeout=30)
        if manifest_resp.status_code == 200:
            content_type = manifest_resp.headers.get("content-type", "")
            if content_type.startswith("application/json") or url.endswith(".json"):
                try:
                    return url, manifest_resp.json()
                except Exception:
                    pass
            if (
                content_type.startswith("application/yaml")
                or content_type.startswith("application/x-yaml")
                or url.endswith(".yaml")
                or url.endswith(".yml")
            ):
                try:
                    return url, yaml.safe_load(manifest_resp.text)
                except Exception:
                    pass
            if manifest_resp.text and len(manifest_resp.text.strip()) > 0:
                return url, {"raw_path": True, "content_type": content_type}
        return None, None
    except Exception:
        return None, None


def prepare_test_combinations(schac_identifier: Optional[str], website_link: Optional[str]) -> List[dict]:
    tested_combinations: set = set()
    test_combinations: List[dict] = []

    if schac_identifier:
        test_combinations.append({"domain": schac_identifier, "type": "DNS", "check": False, "path": None})
        tested_combinations.add((schac_identifier, "DNS"))
        test_combinations.append({"domain": schac_identifier, "type": ".well-known", "check": None, "path": None})
        tested_combinations.add((schac_identifier, ".well-known"))

    if website_link:
        parsed_url = urlparse(website_link)
        if not parsed_url.scheme:
            website_link = f"https://{website_link}"
            parsed_url = urlparse(website_link)

        root_domain = parsed_url.netloc

        if (root_domain, "DNS") not in tested_combinations:
            test_combinations.append({"domain": root_domain, "type": "DNS", "check": None, "path": None})
            tested_combinations.add((root_domain, "DNS"))
        if (root_domain, ".well-known") not in tested_combinations:
            test_combinations.append({"domain": root_domain, "type": ".well-known", "check": None, "path": None})
            tested_combinations.add((root_domain, ".well-known"))

        root_domain_no_www = root_domain.replace("www.", "")

        if (root_domain_no_www, "DNS") not in tested_combinations:
            test_combinations.append({"domain": root_domain_no_www, "type": "DNS", "check": None, "path": None})
            tested_combinations.add((root_domain_no_www, "DNS"))
        if (root_domain_no_www, ".well-known") not in tested_combinations:
            test_combinations.append({"domain": root_domain_no_www, "type": ".well-known", "check": None, "path": None})
            tested_combinations.add((root_domain_no_www, ".well-known"))

    return test_combinations


def safe_release_lock(redis_client, lock_key: str, lock_uuid: str) -> bool:
    lua_script = """
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("del", KEYS[1])
    else
        return 0
    end
    """
    try:
        result = redis_client.eval(lua_script, 1, lock_key, lock_uuid)
        if result == 1:
            print(f"Lock released: {lock_key}")
            return True
        else:
            print(f"Lock not owned or already expired: {lock_key}")
            return False
    except Exception as e:
        print(f"Error releasing lock: {e}")
        return False
