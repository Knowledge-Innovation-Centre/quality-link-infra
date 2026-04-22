"""Transaction ledger for course_fetch runs.

Each call to run_course_fetch creates one row via start_transaction,
updates it via update_transaction as stages complete, and seals it via
finish_transaction. The ledger is observability — errors here must not
break the pipeline, so every function swallow-and-logs.
"""
import logging
from typing import Optional, Tuple
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def start_transaction(
    db: Session,
    provider_uuid: UUID,
    source_version_uuid: UUID,
    source_uuid: UUID,
) -> Optional[Tuple[UUID, int]]:
    """Insert a new 'running' row for today and return (trans_uuid, run_number).

    run_number is MAX(run_number) + 1 over today's rows for this source,
    starting at 1. Returns None if the insert fails (caller should proceed
    without ledger tracking rather than crash).
    """
    try:
        run_number = db.execute(
            text("""
                SELECT COALESCE(MAX(run_number), 0) + 1
                FROM transaction
                WHERE provider_uuid = :p
                  AND source_version_uuid = :v
                  AND source_uuid = :s
                  AND created_at_date = CURRENT_DATE
            """),
            {"p": str(provider_uuid), "v": str(source_version_uuid), "s": str(source_uuid)},
        ).scalar() or 1

        trans_uuid = db.execute(
            text("""
                INSERT INTO transaction
                    (provider_uuid, source_version_uuid, source_uuid,
                     run_number, status, started_at)
                VALUES
                    (:p, :v, :s, :n, 'running', NOW())
                RETURNING trans_uuid
            """),
            {
                "p": str(provider_uuid),
                "v": str(source_version_uuid),
                "s": str(source_uuid),
                "n": run_number,
            },
        ).scalar()
        db.commit()
        return trans_uuid, run_number
    except Exception as e:
        db.rollback()
        logger.warning("start_transaction failed: %s", e)
        return None


def update_transaction(
    db: Session,
    trans_uuid: UUID,
    *,
    bronze_file_path: Optional[str] = None,
    log_file_path: Optional[str] = None,
    course_count: Optional[int] = None,
) -> None:
    """Set any of the provided columns on the transaction row."""
    fields = {}
    if bronze_file_path is not None:
        fields["bronze_file_path"] = bronze_file_path
    if log_file_path is not None:
        fields["log_file_path"] = log_file_path
    if course_count is not None:
        fields["course_count"] = course_count
    if not fields:
        return

    assignments = ", ".join(f"{k} = :{k}" for k in fields)
    params = {**fields, "t": str(trans_uuid)}

    try:
        db.execute(
            text(f"UPDATE transaction SET {assignments} WHERE trans_uuid = :t"),
            params,
        )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning("update_transaction failed: %s", e)


def finish_transaction(
    db: Session,
    trans_uuid: UUID,
    status: str,
    *,
    error_message: Optional[str] = None,
    log_file_path: Optional[str] = None,
) -> None:
    """Seal the transaction with final status, finished_at=NOW(), and
    optionally error_message / log_file_path.
    """
    try:
        db.execute(
            text("""
                UPDATE transaction
                SET status = :status,
                    finished_at = NOW(),
                    error_message = :err,
                    log_file_path = COALESCE(:lp, log_file_path)
                WHERE trans_uuid = :t
            """),
            {
                "status": status,
                "err": error_message,
                "lp": log_file_path,
                "t": str(trans_uuid),
            },
        )
        db.commit()
    except Exception as e:
        db.rollback()
        logger.warning("finish_transaction failed: %s", e)
