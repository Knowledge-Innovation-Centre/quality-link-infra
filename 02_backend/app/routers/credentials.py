from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy import text
from sqlalchemy.orm import Session

from database import get_db

router = APIRouter()


@router.get("/public-key")
async def get_public_key(db: Session = Depends(get_db)):
    try:
        result = db.execute(
            text("""
                SELECT public_key, created_at, updated_at
                FROM ql_cred
                WHERE is_active = TRUE
                ORDER BY created_at DESC
                LIMIT 1
            """)
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="No active public key found")

        return {
            "public_key": result[0],
            "created_at": result[1].isoformat() if result[1] else None,
            "updated_at": result[2].isoformat() if result[2] else None,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving public key: {str(e)}")


@router.get("/public-key/pem", response_class=PlainTextResponse)
async def get_public_key_pem(db: Session = Depends(get_db)):
    try:
        result = db.execute(
            text("""
                SELECT public_key
                FROM ql_cred
                WHERE is_active = TRUE
                ORDER BY created_at DESC
                LIMIT 1
            """)
        ).fetchone()

        if not result:
            raise HTTPException(status_code=404, detail="No active public key found")

        return result[0]

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving public key: {str(e)}")
