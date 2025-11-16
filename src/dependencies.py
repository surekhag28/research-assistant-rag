from typing import Annotated, Generator
from fastapi import Depends, Request
from sqlalchemy.orm import Session
from src.db.interface import BaseDatabase
from src.config import AppSettings


def get_request_settings(request: Request) -> AppSettings:
    return request.app.state.settings


def get_database(request: Request) -> BaseDatabase:
    return request.app.state.database


def get_db_session(
    database: Annotated[BaseDatabase, Depends(get_database)],
) -> Generator[Session, None, None]:
    with database.get_session() as session:
        yield session


SettingsDep = Annotated[AppSettings, Depends(get_request_settings)]
DatabaseDep = Annotated[BaseDatabase, Depends(get_database)]
SessionDep = Annotated[Session, Depends(get_db_session)]
