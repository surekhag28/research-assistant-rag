import os
from pathlib import Path
from typing import Literal, List, Union

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class DefaultSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", frozen=True, env_nested_delimiter="__"
    )


class PostgresSettings(DefaultSettings):
    database_url: str = "postgresql://rag_user:rag_password@localhost:5432/rag_db"
    echo_sql: bool = False
    pool_size: int = 20
    max_overflow: int = 0

    class Config:
        env_prefix = "POSTGRES_"


class OpenSearchSettings(DefaultSettings):
    host: str = "http://localhost:9200"

    class Config:
        env_prefix = "OPENSEARCH_"


class OllamaSettings(DefaultSettings):
    host: str = "http://localhost:11434"
    models: Union[str, List[str]] = Field(default=["gpt-oss:20b", "llama3.2:1b"])
    default_model: str = "llama3.2:1b"
    timeout: int = 300  # seconds

    @field_validator("models", mode="before")
    @classmethod
    def parse_model(cls, v):
        if isinstance(v, str):
            return [model.strip() for model in v.split(",") if model.strip()]
        return v

    class Config:
        env_prefix = "OLLAMA_"


class ArxivSettings(DefaultSettings):
    base_url: str = "https://export.arxiv.org/api/query"
    namespaces: dict = Field(
        default={
            "atom": "http://www.w3.org/2005/Atom",
            "opensearch": "http://a9.com/-/spec/opensearch/1.1/",
            "arxiv": "http://arxiv.org/schemas/atom",
        }
    )

    pdf_cache_dir: str = "./data/arxiv_pdfs"
    rate_limit_delay: float = 3.0
    timeout_seconds: int = 30
    max_results: int = 100
    search_category: str = "cs.AI"


class AppSettings(DefaultSettings):
    app_version: str = "0.1.0"
    debug: bool = True
    environment: str = "development"
    service_name: str = "rag-api"

    postgres: PostgresSettings = PostgresSettings()
    opensearch: OpenSearchSettings = OpenSearchSettings()
    ollama: OllamaSettings = OllamaSettings()
    arxiv: ArxivSettings = ArxivSettings()


# factory to return settings object
def get_settings() -> AppSettings:
    return AppSettings()
