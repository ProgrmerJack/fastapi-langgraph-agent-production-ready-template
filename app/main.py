"""This file contains the main application entry point."""

import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import (
    Any,
    Dict,
)

from dotenv import load_dotenv
from fastapi import (
    FastAPI,
    Request,
    status,
)
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api.v1.api import api_router
from app.api.v1.events import router as events_router
from app.core.config import settings
from app.core.limiter import limiter
from app.core.logging import logger
from app.core.metrics import setup_metrics
from app.core.middleware import MetricsMiddleware
from app.services.database import database_service

# Load environment variables
load_dotenv()

# Initialize Langfuse (optional - gracefully degrade if not available)
langfuse = None
try:
    from langfuse import Langfuse
    langfuse = Langfuse(
        public_key=os.getenv("LANGFUSE_PUBLIC_KEY"),
        secret_key=os.getenv("LANGFUSE_SECRET_KEY"),
        host=os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com"),
    )
except ImportError:
    logger.warning("langfuse not installed - LLM tracing disabled")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown events."""
    logger.info(
        "application_startup",
        project_name=settings.PROJECT_NAME,
        version=settings.VERSION,
        api_prefix=settings.API_V1_STR,
    )
    yield
    logger.info("application_shutdown")


app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    description=settings.DESCRIPTION,
    openapi_url=f"{settings.API_V1_STR}/openapi.json",
    lifespan=lifespan,
)

# Set up Prometheus metrics
setup_metrics(app)

# Add custom metrics middleware
app.add_middleware(MetricsMiddleware)

# Set up rate limiter exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Add validation exception handler
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors from request data.

    Args:
        request: The request that caused the validation error
        exc: The validation error

    Returns:
        JSONResponse: A formatted error response
    """
    # Log the validation error
    logger.error(
        "validation_error",
        client_host=request.client.host if request.client else "unknown",
        path=request.url.path,
        errors=str(exc.errors()),
    )

    # Format the errors to be more user-friendly
    formatted_errors = []
    for error in exc.errors():
        loc = " -> ".join(
            [str(loc_part) for loc_part in error["loc"] if loc_part != "body"]
        )
        formatted_errors.append({"field": loc, "message": error["msg"]})

    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={"detail": "Validation error", "errors": formatted_errors},
    )


# Set up CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routers
app.include_router(api_router, prefix=settings.API_V1_STR)
# Events router at root level for bot telemetry
app.include_router(events_router, tags=["events"])
# Market router at root level for dashboard
from app.api.v1.market import router as market_router
app.include_router(market_router, tags=["market"])


@app.get("/")
@limiter.limit(settings.RATE_LIMIT_ENDPOINTS["root"][0])
async def root(request: Request):
    """Root endpoint returning basic API information with health snapshot."""
    logger.info("root_endpoint_called")
    payload, _ = await _compute_health_payload()
    return {
        **payload,
        "service": settings.PROJECT_NAME,
        "message": "Agent backend online. Use /ready for readiness and /docs for API schema.",
        "endpoints": {
            "health": "/health",
            "ready": "/ready",
            "api_v1": settings.API_V1_STR,
            "docs": "/docs",
            "redoc": "/redoc",
        },
    }




async def _compute_health_payload() -> tuple[Dict[str, Any], int]:
    """Aggregate service health checks into a unified payload."""
    try:
        db_healthy = await database_service.health_check()
    except Exception as exc:  # pragma: no cover - defensive logging
        logger.exception("database_health_check_failed", error=str(exc))
        db_healthy = False
        reason = "Database health check raised an exception"
    else:
        reason = None if db_healthy else "Database connectivity check failed"
        if not db_healthy:
            logger.warning("database_health_check_degraded")

    payload: Dict[str, Any] = {
        "status": "ok" if db_healthy else "degraded",
        "ready": bool(db_healthy),
        "reason": reason,
        "version": settings.VERSION,
        "environment": settings.ENVIRONMENT.value,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "components": {
            "api": "ok",
            "database": "ok" if db_healthy else "unavailable",
        },
    }
    status_code = (
        status.HTTP_200_OK if db_healthy else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return payload, status_code

@app.get("/health")
@limiter.limit(settings.RATE_LIMIT_ENDPOINTS["health"][0])
async def health_check(request: Request) -> JSONResponse:
    """Health check endpoint with environment-specific information."""
    logger.info("health_check_called")
    payload, status_code = await _compute_health_payload()
    return JSONResponse(content=payload, status_code=status_code)


@app.get("/ready")
@limiter.limit(settings.RATE_LIMIT_ENDPOINTS["ready"][0])
async def readiness_check(request: Request) -> JSONResponse:
    """Readiness endpoint aligned with orchestrator expectations."""
    logger.info("ready_check_called")
    payload, status_code = await _compute_health_payload()
    return JSONResponse(content=payload, status_code=status_code)
