import logging
import os

from fastapi import APIRouter, HTTPException, Request
from global_config import GlobalConfig

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/info",
    summary="Get application info",
    description="Returns the application settings and configuration information",
    response_description="A dictionary containing all application settings",
    tags=["Health"],
)
def info(request: Request):
    app_config = request.app.state.app_config
    return {
        "info": request.app.state.settings.model_dump(),
        "app_config": app_config.to_dict() if app_config else None,
        "global_config": GlobalConfig().to_dict(),
        "request_info": {
            "url": str(request.url),
            "method": request.method,
            "headers": dict(request.headers),
            "client_host": request.client.host if request.client else None,
            "base_url": str(request.base_url),
            "scheme": request.url.scheme,
        },
    }


@router.get(
    "/env",
    summary="Get environment variables",
    description="Returns all environment variables available to the application",
    response_description="A dictionary containing all environment variables",
    tags=["Health"],
)
def get_env():
    return {"env": dict(os.environ)}


@router.get(
    "/health",
    summary="Get application health status",
    description="Returns the health status and basic information about the application",
    response_description="A dictionary containing health status and application metadata",
    tags=["Health"],
)
def health_check(request: Request):
    return {
        "status": "healthy",
        "version": request.app.state.settings.version,
        "git_commit": request.app.state.settings.git_commit,
        "release_date": request.app.state.settings.release_date,
        "app_name": request.app.state.settings.app_name,
        "service_name": request.app.state.settings.service_name,
    }


@router.get(
    "/ready",
    summary="Get application readiness status",
    description="Returns the readiness status and checks Pub/Sub connections",
    response_description="A dictionary containing readiness status, Pub/Sub connection status, and application metadata",
    tags=["Health"],
)
def readiness_check(request: Request):
    try:  # Check Pub/Sub connections
        return {
            "status": "ready",
            "pubsub": "connected" if GlobalConfig().publisher else "not connected",
            "version": request.app.state.settings.version,
            "git_commit": request.app.state.settings.git_commit,
            "release_date": request.app.state.settings.release_date,
            "app_name": request.app.state.settings.app_name,
            "service_name": request.app.state.settings.service_name,
        }
    except Exception as e:
        logger.error(f"❌ Readiness check failed: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail={
                "status": "not_ready",
                "error": str(e),
                "version": request.app.state.settings.version,
                "service_name": request.app.state.settings.service_name,
            },
        )
