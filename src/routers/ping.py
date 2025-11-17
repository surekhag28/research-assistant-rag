from fastapi import APIRouter
from sqlalchemy import text

from ..dependencies import SettingsDep, SessionDep, DatabaseDep
from src.schemas.api.health import HealthResponse, ServiceStatus

router = APIRouter()


@router.get("/ping", tags=["Health"])
async def ping():
    return {"status": "ok", "message": "ping"}


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="health check",
    description="Check the health and status of API service including database connectivity",
    response_description="Service health information",
    tags=["Health"],
)
async def health_check(settings: SettingsDep, database: DatabaseDep) -> HealthResponse:

    services = {}
    overall_status = "ok"

    try:
        with database.get_session() as session:
            session.execute(text("SELECT 1"))
            services["database"] = ServiceStatus(
                status="healthy", message="Connected successfully"
            )
    except Exception as e:
        services["database"] = ServiceStatus(
            status="unhealthy", message=f"Connection failed: {str(e)}"
        )
        overall_status = "degraded"

    return HealthResponse(
        status=overall_status,
        version=settings.app_version,
        envrionment=settings.environment,
        service_name=settings.service_name,
        services=services,
    )
