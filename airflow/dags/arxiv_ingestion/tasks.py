import asyncio
import logging
import sys
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Tuple

sys.path.insert(0, "/opt/airflow")

# All imports at the top
from sqlalchemy import text
from src.db.factory import make_database
from src.services.arxiv.factory import make_arxiv_client
from src.services.metadata_fetcher import make_metadata_fetcher
from src.services.pdf_parser.factory import make_pdf_parser_service

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def get_cached_services() -> Tuple[Any, Any, Any, Any]:
    logger.info("Initialising services (cacahed with lru_cache)")

    arxiv_client = make_arxiv_client()
    pdf_parser = make_pdf_parser_service()
    database = make_database()

    metadata_fetcher = make_metadata_fetcher(arxiv_client, pdf_parser)
    logger.info("All services initialised and cached with lru_cache")

    return arxiv_client, pdf_parser, database, metadata_fetcher


async def run_paper_ingestion_pipeline(
    target_date: str, max_results: int = 5, process_pdfs: bool = True
) -> dict:

    _arxiv_client, _pdf_parser, database, metadata_fetcher = get_cached_services()

    with database.get_session() as session:
        return await metadata_fetcher.fetch_and_process_papers(
            max_results=max_results,
            from_date=target_date,
            to_date=target_date,
            process_pdfs=process_pdfs,
            store_to_db=True,
            db_session=session,
        )


def setup_environment():

    logger.info("Setting up environment for arxiv paper ingestion")

    try:
        arxiv_client, pdf_parser, database, metedata_fetcher = get_cached_services()

        with database.get_session() as session:
            session.execute(text("SELECT 1"))
            logger.info("Database connection verified")

        logger.info(f"arxiv client ready: {arxiv_client.base_url}")
        logger.info("PDF parser service ready (Docling models cached)")

        return {"status": "success", "message": "Environment setup completed"}
    except Exception as e:
        error_msg = f"Environment setup failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def fetch_daily_papers(**context):

    logger.info("Starting daily arxiv paper fetch")

    try:
        execution_date = context["ds"]
        execution_dt = datetime.strptime(execution_date, "%Y-%m-%d")
        target_date = execution_dt - timedelta(1)
        target_date = target_date.strftime("%Y%m%d")
        # target_date = "20250127" #for testing purposes
        logger.info(f"Fetching papers for date: {target_date}")

        results = asyncio.run(
            run_paper_ingestion_pipeline(
                target_date=target_date, max_results=20, process_pdfs=True
            )
        )

        logger.info(f"Daily paper fetched completed: {results}")

        context["task_instance"].xcom_push(key="fetch_results", value=results)

        return results
    except Exception as e:
        error_msg = f"Daily paper fetch failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def process_failed_pdfs(**context):

    logger.info("Processing failed PDFs")

    try:
        fetch_results = context["task_instance"].xcom_pull(
            task_ids="fetch_daily_papers", key="fetch_results"
        )

        if not fetch_results and not fetch_results.get("errors"):
            logger.info("No failed PDFs to retry")
            return {"status": "skipped", "message": "No failures to retry"}

        logger.info(f"Found {len(fetch_results['errors'])}")

        for error in fetch_results["errors"]:
            logger.warning(f"Error to investigate: {error}")

        return {
            "status": "analysed",
            "errors_logged": len(fetch_results["errors"]),
            "message": "Errors logged for investigation",
        }
    except Exception as e:
        error_msg = f"Failed PDF processing error: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def create_opensearch_placeholders(**context):

    logger.info("Creating OpenSearch placeholders")

    try:
        fetch_results = context["task_instance"].xcom_pull(
            task_ids="fetch_daily_papers", key="fetch_results"
        )

        if not fetch_results:
            logger.info("No fetched results available for OpenSearch placeholders")
            return {"status": "skipped", "message": "No papers to process"}

        papers_stored = fetch_results.get("papers_stored", 0)
        logger.info(f"Creating placeholders for {papers_stored} papers")

        placeholder_results = {
            "status": "placeholder",
            "papers_ready_for_indexing": papers_stored,
            "message": f"{papers_stored} papers ready for future OpenSearch indexing",
        }

        logger.info(f"OpenSearch placeholders: {placeholder_results}")

        return placeholder_results

    except Exception as e:
        error_msg = f"OpenSearch placeholder creation failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)


def generate_daily_report(**context):

    logger.info("Generating daily processing report")

    try:
        fetch_results = context["task_instance"].xcom_pull(
            task_ids="fetch_daily_paper", key="fetch_results"
        )
        failed_pdf_results = context["task_instance"].xcom_pull(
            task_ids="process_failed_pdfs"
        )
        opensearch_results = context["task_instance"].xcom_pull(
            task_ids="create_opensearch_placeholders"
        )

        report = {
            "date": context["ds"],
            "execution_time": datetime.now().isoformat(),
            "papers": {
                "fetched": (
                    fetch_results.get("papers_fetched", 0) if fetch_results else 0
                ),
                "pdfs_downloaded": (
                    fetch_results.get("pdfs_downloaded", 0) if fetch_results else 0
                ),
                "pdfs_parsed": (
                    fetch_results.get("pdfs_parsed", 0) if fetch_results else 0
                ),
                "stored": fetch_results.get("papers_stored", 0) if fetch_results else 0,
            },
            "processing": {
                "processing_time_seconds": (
                    fetch_results.get("processing_time", 0) if fetch_results else 0
                ),
                "errors": len(fetch_results.get("errors", [])) if fetch_results else 0,
                "failed_pdf_retries": (
                    failed_pdf_results.get("errors_logged", 0)
                    if failed_pdf_results
                    else 0
                ),
            },
            "opensearch": {
                "placeholders_created": (
                    opensearch_results.get("papers_ready_for_indexing", 0)
                    if opensearch_results
                    else 0
                ),
                "status": (
                    opensearch_results.get("status", "unknown")
                    if opensearch_results
                    else "unknown"
                ),
            },
        }

        logger.info("=== DAILY ARXIV PROCESSING REPORT ===")
        logger.info(f"Date: {report['date']}")
        logger.info(f"Papers fetched: {report['papers']['fetched']}")
        logger.info(f"PDFs downloaded: {report['papers']['pdfs_downloaded']}")
        logger.info(f"PDFs parsed: {report['papers']['pdfs_parsed']}")
        logger.info(f"Papers stored: {report['papers']['stored']}")
        logger.info(
            f"Processing time: {report['processing']['processing_time_seconds']:.1f}s"
        )
        logger.info(f"Errors encountered: {report['processing']['errors']}")
        logger.info(
            f"OpenSearch placeholders: {report['opensearch']['placeholders_created']}"
        )
        logger.info("=== END REPORT ===")

        return report

    except Exception as e:
        error_msg = f"Report generation failed: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)
