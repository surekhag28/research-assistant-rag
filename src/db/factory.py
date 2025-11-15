from src.db.interface import BaseDatabase
from src.config import PostgresSettings
from src.db.postgresql import PostgreSQLDatabase


def make_database(settings: PostgresSettings) -> BaseDatabase:

    database = PostgreSQLDatabase(settings)
    database.startup()
    return database
