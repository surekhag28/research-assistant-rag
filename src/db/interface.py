from abc import ABC, abstractmethod
from typing import Any, ContextManager, Dict, List, Optional
from sqlalchemy.orm import Session


class BaseDatabase(ABC):

    @abstractmethod
    def startup(self) -> None:
        """Initialize the database connection"""

    @abstractmethod
    def teardown(self) -> None:
        """Close the database connection"""

    @abstractmethod
    def get_session(self) -> ContextManager[Session]:
        """Get as database session"""
