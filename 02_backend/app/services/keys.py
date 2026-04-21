import hashlib
import logging
from typing import Optional
from uuid import UUID

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def active_key_exists(db: Session) -> bool:
    row = db.execute(
        text("SELECT 1 FROM ql_cred WHERE is_active = TRUE LIMIT 1")
    ).fetchone()
    return row is not None


def generate_and_store_keypair(
    db: Session,
    *,
    key_size: int = 4096,
    public_exponent: int = 65537,
) -> Optional[UUID]:
    """Generate an RSA keypair, mark any existing key inactive, and insert the
    new one. Returns the new cred_uuid, or None if the insert failed.
    """
    private_key = rsa.generate_private_key(
        public_exponent=public_exponent,
        key_size=key_size,
    )
    public_key = private_key.public_key()

    private_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    public_pem = public_key.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    fingerprint = hashlib.sha256(public_pem).hexdigest()

    db.execute(text("UPDATE ql_cred SET is_active = FALSE WHERE is_active = TRUE"))
    row = db.execute(
        text("""
            INSERT INTO ql_cred
            (public_key, private_key, key_algorithm, key_size, public_exponent, key_format)
            VALUES
            (:public_key, :private_key, 'RSA', :key_size, :public_exponent, 'PEM')
            RETURNING cred_uuid
        """),
        {
            "public_key": public_pem.decode("utf-8"),
            "private_key": private_pem.decode("utf-8"),
            "key_size": key_size,
            "public_exponent": public_exponent,
        },
    ).fetchone()
    db.commit()

    cred_uuid = row[0] if row else None
    logger.info("Generated new RSA keypair %s (fingerprint=%s)", cred_uuid, fingerprint)
    return cred_uuid


def ensure_active_keypair(db: Session) -> None:
    """Called at app startup. Generates a keypair iff no active one exists."""
    if active_key_exists(db):
        logger.info("Active RSA keypair present in ql_cred — no key generation needed")
        return
    logger.info("No active RSA keypair found — generating one")
    generate_and_store_keypair(db)
