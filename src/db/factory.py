from src.db.interface import BaseDatabase
from src.config import PostgresSettings
from src.db.postgresql import PostgreSQLDatabase
from src.config import get_settings


def make_database() -> BaseDatabase:

    settings = get_settings()
    database = PostgreSQLDatabase(settings.postgres)
    database.startup()
    return database
