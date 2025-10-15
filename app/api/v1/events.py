"""Events API router for bot telemetry and SSE streaming."""

import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, Deque, Optional

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.core.logging import logger
from prometheus_client import Counter, Gauge, Histogram

router = APIRouter()

# In-memory event buffer for SSE streaming
_event_buffer: Deque[Dict[str, Any]] = deque(maxlen=1000)
_event_counter = 0
_subscribers: set[int] = set()

# Prometheus instrumentation
_event_kind_counter = Counter(
    "agent_events_total", "Events received by kind", ["kind"]
)
_event_type_counter = Counter(
    "agent_events_by_type_total", "Events emitted to SSE by type", ["type"]
)
_sse_subscriber_gauge = Gauge(
    "agent_events_sse_subscribers", "Number of active SSE subscribers"
)

# Additional Prometheus metrics for comprehensive observability
_sse_lag_histogram = Histogram(
    "sse_lag_seconds",
    "Latency between event creation and SSE emission",
    buckets=[0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0]
)

_stream_reconnects_counter = Counter(
    "stream_reconnects_total",
    "Number of SSE stream reconnection attempts",
    ["reason"]
)

_actions_denied_counter = Counter(
    "actions_denied_total",
    "Number of actions denied by risk controller",
    ["reason"]
)

_circuit_breaker_state_gauge = Gauge(
    "circuit_breaker_state",
    "Circuit breaker state (0=closed, 1=open, 2=half-open)",
    ["service"]
)


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


def _normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    if not symbol:
        return symbol
    normalized = symbol.replace("/", "-").replace("_", "-")
    return normalized.upper()


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _timestamp_ms(value: Optional[Any]) -> int:
    if value is None:
        return int(datetime.now(timezone.utc).timestamp() * 1000)
    if isinstance(value, (int, float)):
        return int(float(value))
    candidate = str(value)
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        try:
            return int(float(candidate))
        except ValueError:
            return int(datetime.now(timezone.utc).timestamp() * 1000)
    return int(parsed.timestamp() * 1000)


def _enrich_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Augment raw bot events with chart-friendly fields."""
    metadata = event.get("metadata") or {}
    kind = str(event.get("kind") or "").lower()

    ts_source = (
        metadata.get("timestamp_ms")
        or metadata.get("timestamp")
        or event.get("timestamp")
        or event.get("received_at")
    )
    event["t"] = _timestamp_ms(ts_source)
    original_symbol = event.get("symbol")
    if "original_symbol" not in event and original_symbol is not None:
        event["original_symbol"] = original_symbol
    event["symbol"] = _normalize_symbol(original_symbol)

    if kind == "signal_generated":
        event["type"] = "signal"
        signal_action = (
            event.get("action")
            or metadata.get("signal")
            or metadata.get("action")
            or "hold"
        )
        event["signal"] = str(signal_action).lower()
        event["source"] = metadata.get("source", "strategy")
        confidence = (
            event.get("confidence")
            if event.get("confidence") is not None
            else metadata.get("confidence")
        )
        if confidence is None and event.get("meta_score") is not None:
            confidence = event.get("meta_score")
        event["conf"] = _coerce_float(confidence, 0.0)
        _event_type_counter.labels(type="signal").inc()

    elif kind == "order_filled":
        event["type"] = "trade"
        event["side"] = str(event.get("side") or metadata.get("side") or "").lower()
        event["px"] = _coerce_float(event.get("price") or metadata.get("price"))
        event["qty"] = _coerce_float(event.get("amount") or metadata.get("amount"))
        order_ref = event.get("order_id") or metadata.get("order_id")
        if order_ref:
            event["orderId"] = order_ref
        _event_type_counter.labels(type="trade").inc()

    elif kind == "order_placed":
        event.setdefault("type", "order")
        event.setdefault("order_status", "placed")
        _event_type_counter.labels(type="order").inc()

    elif kind == "error":
        event.setdefault("type", "error")
        _event_type_counter.labels(type="error").inc()

    else:
        # Leave unclassified events untouched but still track
        event.setdefault("type", kind or "unknown")
        _event_type_counter.labels(type=event["type"]).inc()

    return event


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

    # Normalize event payload for downstream consumers
    kind_label = str(event.get("kind") or "unknown")
    event = _enrich_event(event)

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

    _event_kind_counter.labels(kind=kind_label).inc()

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
    _sse_subscriber_gauge.set(len(_subscribers))

    logger.info(
        "sse_client_connected",
        subscriber_id=subscriber_id,
        total_subscribers=len(_subscribers),
    )

    async def event_generator():
        """Generate SSE events."""
        # Send initial buffer to new subscriber
        for event in _event_buffer:
            evt_type = event.get("type", "message")
            yield f"event: {evt_type}\ndata: {_format_event(event)}\n\n"

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
                            evt_type = event.get("type", "message")
                            yield f"event: {evt_type}\ndata: {_format_event(event)}\n\n"
                    last_sent_id = current_id

        finally:
            _subscribers.discard(subscriber_id)
            _sse_subscriber_gauge.set(len(_subscribers))
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

