import os
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from src.config import get_settings, AppSettings
from src.db.factory import make_database
from src.routers import ping

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting RAG API.....")

    settings = get_settings()
    app.state.settings = settings

    database = make_database(settings.postgres)
    app.state.database = database
    logger.info("Database connected")

    # other services
    yield

    database.teardown()
    logger.info("API shutdown completely")


app = FastAPI(
    title="Research Assistant RAG",
    description="Personal research assistant for arxiv papers with RAG capabilities",
    version=os.getenv("APP_VERSION", "0.1.0"),
    lifespan=lifespan,
)

app.include_router(ping.router, prefix="/api/v1")

if __name__ == "__main__":
    uvicorn.run(app, port=8000, host="0.0.0.0")
