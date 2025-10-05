"""Events API router for bot telemetry and SSE streaming."""

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, Deque

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.logging import logger

router = APIRouter()

# In-memory event buffer for SSE streaming
_event_buffer: Deque[Dict[str, Any]] = deque(maxlen=1000)
_event_counter = 0
_subscribers: set[int] = set()


class Event(BaseModel):
    """Event schema for bot telemetry."""

    kind: str
    symbol: str
    timestamp: str
    metadata: Dict[str, Any] = {}


class SignalEvent(Event):
    """Trading signal event."""

    action: str
    price: float
    size: float


class OrderEvent(Event):
    """Order event."""

    side: str
    order_id: str
    price: float
    qty: float
    status: str


class FillEvent(Event):
    """Fill event."""

    side: str
    fill_price: float
    fill_qty: float
    order_id: str


class DecisionEvent(Event):
    """Decision event."""

    action: str
    reason: str


class MarketDataEvent(Event):
    """Market data event."""

    price: float
    volume: float | None = None


class HealthEvent(Event):
    """Health status event."""

    status: str
    uptime_seconds: float


@router.post("/events/publish")
async def publish_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Receive an event from the trading bot and add it to the SSE stream.

    Args:
        event: Event payload from the bot

    Returns:
        Confirmation of event receipt
    """
    global _event_counter

    # Add metadata
    event.setdefault("id", _event_counter)
    event.setdefault("received_at", datetime.now(timezone.utc).isoformat())

    _event_counter += 1

    # Add to buffer
    _event_buffer.append(event)

    logger.info(
        "event_published",
        event_kind=event.get("kind"),
        event_id=event.get("id"),
        symbol=event.get("symbol"),
        buffer_size=len(_event_buffer),
        subscribers=len(_subscribers),
    )

    return {
        "status": "ok",
        "event_id": event.get("id"),
        "subscribers_notified": len(_subscribers),
    }


@router.get("/events/recent")
async def get_recent_events(limit: int = 100) -> Dict[str, Any]:
    """
    Get recent events from the buffer.

    Args:
        limit: Maximum number of events to return

    Returns:
        Recent events and buffer stats
    """
    events = list(_event_buffer)[-limit:]

    return {
        "events": events,
        "count": len(events),
        "total_in_buffer": len(_event_buffer),
        "buffer_maxlen": _event_buffer.maxlen,
    }


@router.get("/events/stream")
async def stream_events(request: Request) -> StreamingResponse:
    """
    Server-Sent Events (SSE) endpoint for real-time event streaming.

    This endpoint streams all events published via /events/publish to connected clients.
    The dashboard connects to this endpoint to receive real-time bot updates.

    Returns:
        StreamingResponse with text/event-stream content type
    """
    subscriber_id = id(request)
    _subscribers.add(subscriber_id)

    logger.info(
        "sse_client_connected",
        subscriber_id=subscriber_id,
        total_subscribers=len(_subscribers),
    )

    async def event_generator():
        """Generate SSE events."""
        # Send initial buffer to new subscriber
        for event in _event_buffer:
            yield f"data: {_format_event(event)}\n\n"

        # Stream new events as they arrive
        last_sent_id = _event_counter - 1

        try:
            while True:
                await asyncio.sleep(0.5)  # Check for new events every 500ms

                # Check if client disconnected
                if await request.is_disconnected():
                    break

                # Send new events
                current_id = _event_counter - 1
                if current_id > last_sent_id:
                    for event in _event_buffer:
                        if event.get("id", -1) > last_sent_id:
                            yield f"data: {_format_event(event)}\n\n"
                    last_sent_id = current_id

        finally:
            _subscribers.discard(subscriber_id)
            logger.info(
                "sse_client_disconnected",
                subscriber_id=subscriber_id,
                total_subscribers=len(_subscribers),
            )

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",  # Disable nginx buffering
        },
    )


def _format_event(event: Dict[str, Any]) -> str:
    """Format event as JSON string for SSE."""
    import json

    return json.dumps(event)


@router.get("/events/stats")
async def get_event_stats() -> Dict[str, Any]:
    """
    Get statistics about the event buffer and subscribers.

    Returns:
        Event buffer statistics
    """
    return {
        "buffer_size": len(_event_buffer),
        "buffer_maxlen": _event_buffer.maxlen,
        "total_events": _event_counter,
        "active_subscribers": len(_subscribers),
        "latest_event": _event_buffer[-1] if _event_buffer else None,
    }

