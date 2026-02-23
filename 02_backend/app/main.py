from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from config import SERVICE_URL_FRONTEND
from routers import credentials, datalake, health, manifest, providers

app = FastAPI(title="QL-Backend")

origins = [
    "http://localhost:3000",
    "http://frontend:3000",
    SERVICE_URL_FRONTEND,
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router)
app.include_router(providers.router)
app.include_router(manifest.router)
app.include_router(datalake.router)

# Public-key sub-app — wildcard CORS so any provider domain can fetch the key
public_app = FastAPI()
public_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)
public_app.include_router(credentials.router)
app.mount("/api/v1", public_app)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
