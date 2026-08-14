import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import SERVICE_URL_FRONTEND
from database import SessionLocal
from routers import credentials, datalake, health, manifest, providers, search
from services.keys import ensure_active_keypair

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

PUBLIC_PREFIX = "/api/v1"


class ScopedCORSMiddleware(CORSMiddleware):
    """CORS for the main app only, leaving the public sub-app to apply its own.

    The outermost CORSMiddleware answers every preflight itself — including ones
    aimed at the mounted sub-app, which it would reject with 400 for any origin
    outside `origins`. Passing those requests straight through lets the public
    sub-app's wildcard CORS handle them.
    """

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"].startswith(PUBLIC_PREFIX):
            await self.app(scope, receive, send)
            return
        await super().__call__(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI):
    with SessionLocal() as db:
        ensure_active_keypair(db)
    yield


app = FastAPI(title="QL-Backend", lifespan=lifespan)

origins = [
    "http://localhost:3000",
    "http://frontend:3000",
    SERVICE_URL_FRONTEND,
]

app.add_middleware(
    ScopedCORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(providers.router)
app.include_router(manifest.router)
app.include_router(datalake.router)

# Public sub-app — wildcard CORS, no credentials. Only intentionally public,
# unauthenticated, read-only routes belong here: the QL public key (fetched by
# provider domains) and the read-only Meilisearch search proxy.
public_app = FastAPI()
public_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
public_app.include_router(credentials.router)
public_app.include_router(search.router)
app.mount(PUBLIC_PREFIX, public_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
