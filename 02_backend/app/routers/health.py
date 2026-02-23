from fastapi import APIRouter
from sqlalchemy import create_engine, text

from config import DATABASE_URL, DB_USER

router = APIRouter()


@router.get("/")
async def root():
    return {"message": "Database connection testing API is running. Use /health/database to test connection."}


@router.get("/health/database")
async def check_database_connection():
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            result.fetchone()
        return {
            "status": "success",
            "message": "Database connection is healthy",
            "database": DB_USER,
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Database connection failed: {str(e)}",
            "database": DB_USER,
        }
