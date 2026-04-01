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
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="OPENSEARCH__",
        extra="ignore",
        frozen=True,
        case_sensitive=False,
    )

    host: str = "http://localhost:9200"
    index_name: str = "arxiv-papers"
    max_text_size: int = 1000000


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
    download_max_retries: int = 3
    download_retry_delay_base: int = 5
    max_concurrent_downloads: int = 5
    max_concurrent_parsing: int = 1

    @field_validator("pdf_cache_dir")
    @classmethod
    def validate_cache_dir(cls, v: str) -> str:
        os.makedirs(v, exist_ok=True)
        return v


class PDFParserSettings(DefaultSettings):
    max_pages: int = 30
    max_file_size_mb: int = 20
    do_ocr: bool = False
    do_table_structure: bool = True


class Settings(DefaultSettings):
    app_version: str = "0.1.0"
    debug: bool = True
    environment: str = "development"
    service_name: str = "rag-api"

    postgres: PostgresSettings = Field(default_factory=PostgresSettings)
    opensearch: OpenSearchSettings = Field(default_factory=OpenSearchSettings)
    ollama: OllamaSettings = Field(default_factory=OllamaSettings)
    arxiv: ArxivSettings = Field(default_factory=ArxivSettings)
    pdf_parser: PDFParserSettings = Field(default_factory=PDFParserSettings)


# factory to return settings object
def get_settings() -> Settings:
    return Settings()
