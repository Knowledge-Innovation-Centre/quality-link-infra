"""Postgres advisory-lock helpers.

We use the two-key `pg_try_advisory_lock(ns, key)` variant so different lock
types live in different namespaces and never collide even if their
`hashtext()` values happen to match. Locks are session-scoped (released when
the PG connection ends), not transaction-scoped — pull_manifest commits
mid-flight, so an xact lock would be released too early. We always unlock
explicitly; connection death is the belt-and-braces backstop.
"""

import logging
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Namespace constants. Stable integers — do not renumber once deployed.
NS_PULL_MANIFEST = 1
NS_COURSE_FETCH = 2


def try_acquire(db: Session, namespace: int, key: str) -> bool:
    """Try to acquire an advisory lock on (namespace, hashtext(key)). Returns
    True if acquired, False if another session holds it.
    """
    row = db.execute(
        text("SELECT pg_try_advisory_lock(:ns, hashtext(:key))"),
        {"ns": namespace, "key": key},
    ).fetchone()
    return bool(row and row[0])


def release(db: Session, namespace: int, key: str) -> bool:
    """Release the advisory lock on (namespace, hashtext(key)). Returns True
    if we held it, False if we did not (idempotent-safe).
    """
    row = db.execute(
        text("SELECT pg_advisory_unlock(:ns, hashtext(:key))"),
        {"ns": namespace, "key": key},
    ).fetchone()
    return bool(row and row[0])


@contextmanager
def advisory_lock(db: Session, namespace: int, key: str) -> Iterator[bool]:
    """Acquire a session-scoped advisory lock; release it on exit.

    Yields True if the lock was acquired, False if another session holds it.
    Callers must check the yielded value and skip their protected work when
    False:

        with advisory_lock(db, NS_FOO, key) as acquired:
            if not acquired:
                return  # or report busy
            ...

    Contract: any DB writes inside the block must be committed before the
    block exits. On exit the session is rolled back unconditionally so the
    unlock SQL runs on a clean session and can't be poisoned by a failed
    transaction — which means uncommitted writes are discarded.
    """
    acquired = try_acquire(db, namespace, key)
    try:
        yield acquired
    finally:
        if acquired:
            try:
                db.rollback()
            except Exception:
                logger.warning(
                    "advisory_lock: pre-release rollback failed for ns=%s key=%s",
                    namespace, key, exc_info=True,
                )
            try:
                release(db, namespace, key)
            except Exception:
                logger.warning(
                    "advisory_lock: release failed for ns=%s key=%s",
                    namespace, key, exc_info=True,
                )


def is_locked(db: Session, namespace: int, key: str) -> bool:
    """Check whether any session currently holds the advisory lock on
    (namespace, hashtext(key)). Read-only, does not acquire.
    """
    row = db.execute(
        text("""
            SELECT EXISTS (
                SELECT 1 FROM pg_locks
                WHERE locktype = 'advisory'
                  AND classid = :ns
                  AND objid = hashtext(:key)::int
                  AND objsubid = 2
            )
        """),
        {"ns": namespace, "key": key},
    ).fetchone()
    return bool(row and row[0])
