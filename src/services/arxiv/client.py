import asyncio
import logging
import time
from functools import cached_property
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote, urlencode

import httpx
from src.config import ArxivSettings
from src.exceptions import (
    ArxivAPIException,
    ArxivAPITimeoutError,
    ArxivAPIRateLimitError,
    ArxivParseError,
    PDFDownloadException,
    PDFDownloadTimeoutError,
)
from src.schemas.arxiv.paper import ArxivPaper

logger = logging.getLogger(__name__)


class ArxivClient:
    """Client for fetching papers from arxiv API."""

    def __int__(self, settings: ArxivSettings):
        self._settings = settings
        self._last_request_time: Optional[float] = None

    @cached_property
    def pdf_cache_dir(self) -> Path:
        cache_dir = Path(self._settings.pdf_cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir

    @property
    def base_url(self) -> str:
        return self._settings.base_url

    @property
    def namespaces(self) -> dict:
        return self._settings.namespaces

    @property
    def rate_limit_delay(self) -> float:
        return self._settings.rate_limit_delay

    @property
    def timeout_seconds(self) -> int:
        return self._settings.timeout_seconds

    @property
    def max_results(self) -> int:
        return self._settings.max_results

    @property
    def search_category(self) -> str:
        return self._settings.search_category

    async def fetch_papers(
        self,
        max_results: Optional[int] = None,
        start: int = 0,
        sort_by: str = "submittedDate",
        sort_order: str = "descending",
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
    ) -> List[ArxivPaper]:

        if max_results is None:
            max_results = self.max_results

        search_query = f"cat:{self.search_category}"

        if from_date or to_date:
            date_from = f"{from_date}0000" if from_date else "*"
            date_to = f"{to_date}2359" if to_date else "*"
            search_query += f" AND submittedDate: [{date_from}+TO+{date_to}]"

        params = {
            "search_query": search_query,
            "start": start,
            "max_results": min(max_results, 2000),
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }
        safe = ":+[]"
        url = f"{self.base_url}?{urlencode(params,quote_via=quote,safe=safe)}"

        try:
            logger.info(
                f"Fetching {max_results} {self.search_category} papers from arxiv"
            )

            if self._last_request_time is not None:
                time_since_last = time.time() - self._last_request_time
                if time_since_last < self.rate_limit_delay:
                    sleep_time = self.rate_limit_delay - time_since_last
                    await asyncio.sleep(sleep_time)

            self._last_request_time = time.time()

            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(url)
                response.raise_for_status()
                xml_data = response.text

            papers = self._parse_response(xml_data)
            logger.info(f"Fetched {len(papers)} papers")

            return papers
        except httpx.TimeoutException as e:
            logger.error(f"arxiv API timeout: {e}")
            raise ArxivAPITimeoutError(f"arxiv API request timed out: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"arxiv API HTTP error: {e}")
            raise ArxivAPIException(f"arxiv API returned error: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch papers and metadata from arxiv: {e}")
            raise ArxivAPIException(f"Unexpected error fetching papers from arxiv: {e}")

    async def fetch_papers_with_query(
        self,
        search_query: str,
        max_results: Optional[int] = None,
        start: int = 0,
        sort_by: str = "submittedDate",
        sort_order: str = "descending",
    ) -> List[ArxivPaper]:

        if max_results is None:
            max_results = self.max_results

        params = {
            "search_query": search_query,
            "start": start,
            "max_results": min(max_results, 2000),
            "sortBy": sort_by,
            "sortOrder": sort_order,
        }

        safe = ":+[]*"
        url = f"{self.base_url}?{urlencode(params,quote_via=quote,safe=safe)}"

        try:
            if self._last_request_time is not None:
                time_since_last = time.time() - self._last_request_time
                if time_since_last < self.rate_limit_delay:
                    sleep_time = self.rate_limit_delay - time_since_last
                    await asyncio.sleep(sleep_time)

            self._last_request_time = time.time()

            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(url)
                response.raise_for_status()
                xml_data = response.text

            papers = self._parse_response(xml_data)
            logger.info(f"Query returned {len(papers)} papers")

            return papers
        except httpx.TimeoutException as e:
            logger.error(f"arxiv API timeout: {e}")
            raise ArxivAPITimeoutError(f"arxiv API request timed out: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"arxiv API HTTP error: {e}")
            raise ArxivAPIException(f"arxiv API returned error: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch papers and metadata from arxiv: {e}")
            raise ArxivAPIException(f"Unexpected error fetching papers from arxiv: {e}")

    async def fetch_paper_by_id(self, arxiv_id: str) -> Optional[ArxivPaper]:

        # remove version from arxiv id for search
        clean_id = arxiv_id.split("v")[0] if "v" in arxiv_id else arxiv_id
        params = {"id_list": clean_id, "max_results": 1}

        safe = ":+[]*"
        url = f"{self.base_url}?{urlencode(params,quote_via=quote,safe=safe)}"

        try:
            async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
                response = await client.get(url)
                response.raise_for_status()
                xml_data = response.text

            papers = self._parse_response(xml_data)
            logger.info(f"Query returned {len(papers)} papers")

            return papers
        except httpx.TimeoutException as e:
            logger.error(f"arxiv API timeout: {e}")
            raise ArxivAPITimeoutError(f"arxiv API request timed out: {e}")
        except httpx.HTTPStatusError as e:
            logger.error(f"arxiv API HTTP error: {e}")
            raise ArxivAPIException(f"arxiv API returned error: {e}")
        except Exception as e:
            logger.error(f"Failed to fetch papers and metadata from arxiv: {e}")
            raise ArxivAPIException(f"Unexpected error fetching papers from arxiv: {e}")
