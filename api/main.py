import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from routers import events, search, rag
from services.db import init_db
from services.vectorizer import init_vectorizer

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_vectorizer()
    yield

app = FastAPI(
    title="Conflict Monitor API",
    description="Real-time US-Iran conflict intelligence platform",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(events.router, prefix="/api/events", tags=["Events"])
app.include_router(search.router, prefix="/api/search", tags=["Search"])
app.include_router(rag.router, prefix="/api/rag", tags=["RAG"])

@app.get("/health")
def health():
    return {"status": "ok", "service": "conflict-monitor-api"}