import logging
from contextlib import contextmanager
from typing import ContextManager, Generator, Optional

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from src.db.interface import BaseDatabase
from src.config import PostgresSettings


logger = logging.getLogger(__name__)
Base = declarative_base()


class PostgreSQLDatabase(BaseDatabase):

    def __init__(self, config: PostgresSettings) -> None:
        self.config = config
        self.engine: Optional[Engine] = None
        self.session_factory: Optional[sessionmaker] = None

    def startup(self) -> None:
        try:
            logger.info(
                f"Attempting to connect to PostgreSQL at: {self.config.database_url.split('@')[1] if '@' in self.config.database_url else 'localhost'}"
            )
            self.engine = create_engine(
                self.config.database_url,
                echo=self.config.echo_sql,
                pool_size=self.config.pool_size,
                max_overflow=self.config.max_overflow,
                pool_pre_ping=True,  # to verify connections befor use
            )
            self.session_factory = sessionmaker(
                bind=self.engine, expire_on_commit=False
            )

            # test the connection
            assert self.engine is not None
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("Database connection test successfully")

            # check which tables exist before creating
            inspector = inspect(self.engine)
            existing_tables = inspector.get_table_names()

            # creating tables
            Base.metadata.create_all(bind=self.engine)
            updated_tables = inspector.get_table_names()
            new_tables = set(updated_tables) - set(existing_tables)

            if new_tables:
                logger.info(f"Created new tables: {', '.join(new_tables)}")
            else:
                logger.info("All tables already exist - no new tables created")

            logger.info("PostgreSQL database initialized successfully")
            assert self.engine is not None
            logger.info(f"Database: {self.engine.url.database}")
            logger.info(
                f"Total tables: {', '.join(updated_tables) if updated_tables else 'None'}"
            )
            logger.info("Database connection established")
        except Exception as e:
            logger.error(f"Failed to initialise Postgres SQL database")

    def teardown(self) -> None:
        if self.engine:
            self.engine.dispose()
            logger.info("PostgreSQL database coneections closed")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        if not self.session_factory:
            raise RuntimeError("Database not initialised. Call startup() first ")

        session = self.session_factory()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
