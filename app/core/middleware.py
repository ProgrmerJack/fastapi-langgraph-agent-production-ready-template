"""Custom middleware for tracking metrics and other cross-cutting concerns."""

import asyncio
import time
from typing import Callable

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response, JSONResponse

from app.core.metrics import (
    http_requests_total,
    http_request_duration_seconds,
)
from app.core.logging import logger


class MetricsMiddleware(BaseHTTPMiddleware):
    """Middleware for tracking HTTP request metrics.
    
    This middleware tracks request duration and status codes for Prometheus metrics.
    It handles client disconnections and other errors gracefully.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Track metrics for each request.

        Args:
            request: The incoming request
            call_next: The next middleware or route handler

        Returns:
            Response: The response from the application
        """
        start_time = time.time()
        status_code = 500  # Default to error status if something goes wrong
        response = None

        try:
            # Wrap call_next to catch any exceptions during response streaming
            response = await call_next(request)
            status_code = response.status_code
            return response
        except asyncio.CancelledError:
            # Client disconnected - this is expected behavior, don't log as error
            status_code = 499  # Client closed request (nginx-style code)
            # Return empty response for cancelled requests
            return Response(status_code=499, content=b"")
        except GeneratorExit:
            # Generator was closed (client disconnect during streaming)
            status_code = 499
            return Response(status_code=499, content=b"")
        except RuntimeError as exc:
            # Handle "No response returned" and similar runtime errors
            if "No response returned" in str(exc) or "Expected ASGI" in str(exc):
                status_code = 500
                logger.warning(
                    "middleware_no_response",
                    method=request.method,
                    path=request.url.path,
                    error=str(exc),
                )
                return JSONResponse(
                    status_code=500,
                    content={"detail": "Request processing failed"},
                )
            raise
        except Exception as exc:
            # Log unexpected errors but don't fail silently
            status_code = 500
            logger.error(
                "middleware_exception",
                method=request.method,
                path=request.url.path,
                error=str(exc),
                error_type=type(exc).__name__,
            )
            # Return a proper error response instead of crashing
            return JSONResponse(
                status_code=500,
                content={"detail": "Internal server error", "error_type": type(exc).__name__},
            )
        finally:
            duration = time.time() - start_time

            # Record metrics safely
            try:
                http_requests_total.labels(
                    method=request.method,
                    endpoint=request.url.path,
                    status=status_code,
                ).inc()

                http_request_duration_seconds.labels(
                    method=request.method, endpoint=request.url.path
                ).observe(duration)
            except Exception as metrics_exc:
                # Don't let metrics recording failures break the request
                logger.warning(
                    "metrics_recording_failed",
                    error=str(metrics_exc),
                )
