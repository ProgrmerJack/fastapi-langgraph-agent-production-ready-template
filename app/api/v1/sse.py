"""
Server-Sent Events (SSE) endpoints for real-time market and bot data streaming.
The implementation streams data from Redis when available and gracefully
falls back to polling while providing backfill, heartbeats, and Last-Event-ID
support.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, Optional, Tuple

import httpx
from fastapi import APIRouter, Query, Request
from fastapi.responses import StreamingResponse

from app.api.v1.market import get_candle_storage
from app.core.logging import logger

router = APIRouter()

try:  # pragma: no cover - optional dependency
    import redis.asyncio as redis  # type: ignore
    REDIS_AVAILABLE = True
except ImportError:  # pragma: no cover - redis optional for tests
    REDIS_AVAILABLE = False
    logger.warning("redis.asyncio not available - SSE will use HTTP polling fallback")


def _now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _normalize_symbol(symbol: str) -> str:
    return symbol.replace("-", "").replace("/", "").replace("_", "").upper()


def _parse_event_timestamp(last_event_id: Optional[str]) -> Optional[int]:
    if not last_event_id:
        return None
    try:
        return int(str(last_event_id).split(":")[-1])
    except (TypeError, ValueError):
        return None


class SSEProducer:
    """Produces Server-Sent Events for real-time updates."""

    def __init__(self) -> None:
        self.heartbeat_interval = 15.0
        self.coalesce_window = 0.25
        self.default_retry_ms = 10_000
        self.redis_url = os.getenv("REDIS_URL", "redis://localhost:6379")
        agent_base = os.getenv("AGENT_BACKEND_URL", "http://127.0.0.1:8601").rstrip("/")
        self._events_recent_url = f"{agent_base}/events/recent"
        self._market_backfill_limit = 500

    # ------------------------------------------------------------------
    # Formatting helpers
    # ------------------------------------------------------------------
    def _format_sse(self, event_type: str, data: Dict[str, Any], event_id: Optional[str] = None) -> str:
        parts = []
        if event_id:
            parts.append(f"id: {event_id}")
        parts.append(f"event: {event_type}")
        parts.append(f"data: {json.dumps(data)}")
        return "\n".join(parts) + "\n\n"

    def _retry_frame(self) -> str:
        return f"retry: {self.default_retry_ms}\n\n"

    def _heartbeat_frame(self, last_event_id: Optional[str]) -> str:
        payload: Dict[str, Any] = {"t": _now_ms()}
        if last_event_id:
            payload["lastEventId"] = last_event_id
        return self._format_sse("ping", payload, last_event_id)

    def _ensure_event_identity(self, event: Dict[str, Any], *, prefix: str) -> Tuple[str, int]:
        ts = int(event.get("t") or _now_ms())
        event_id = event.get("id")
        if not event_id:
            event_id = f"{prefix}:{ts}"
            event["id"] = event_id
        return str(event_id), ts

    # ------------------------------------------------------------------
    # Market stream helpers
    # ------------------------------------------------------------------
    async def _backfill_market(
        self,
        symbol: str,
        interval: str,
        resume_after: Optional[int],
    ) -> AsyncGenerator[Tuple[str, int, str], None]:
        try:
            storage = await get_candle_storage()
        except Exception as exc:  # pragma: no cover - storage initialisation failure
            logger.debug("market_backfill_unavailable", error=str(exc))
            return

        candles = await storage.get_candles(
            symbol=symbol,
            timeframe=interval,
            limit=self._market_backfill_limit,
        )
        if not candles:
            return

        normalized_symbol = _normalize_symbol(symbol)
        for candle in reversed(candles):
            ts_ms = int(candle["timestamp"].timestamp() * 1000)
            if resume_after is not None and ts_ms <= resume_after:
                continue
            event = {
                "type": "tick",
                "t": ts_ms,
                "tf": interval,
                "symbol": normalized_symbol,
                "displaySymbol": symbol,
                "o": float(candle["open"]),
                "h": float(candle["high"]),
                "l": float(candle["low"]),
                "c": float(candle["close"]),
                "v": float(candle["volume"]),
            }
            event_id = f"{normalized_symbol}:{interval}:{ts_ms}"
            event["id"] = event_id
            yield self._format_sse("market", event, event_id), ts_ms, event_id

    async def _redis_market_stream(
        self,
        normalized_symbol: str,
        display_symbol: str,
        interval: str,
        resume_after: Optional[int],
        last_event_id: Optional[str],
    ) -> AsyncGenerator[Tuple[str, Optional[int], Optional[str]], None]:
        redis_client = None
        pubsub = None
        pending_tick: Optional[Tuple[Dict[str, Any], str, int]] = None
        last_flush = time.monotonic()
        last_heartbeat = time.monotonic()
        seen_after = resume_after
        current_last_id = last_event_id

        try:
            redis_client = redis.from_url(self.redis_url, decode_responses=True)
            pubsub = redis_client.pubsub()
            channels = [
                f"market:kline:{normalized_symbol}:{interval}",
                f"market:trade:{normalized_symbol}",
            ]
            await pubsub.subscribe(*channels)
            logger.info("market_sse_subscribed", extra={"channels": channels})

            while True:
                now = time.monotonic()
                if pending_tick and now - last_flush >= self.coalesce_window:
                    event, event_id, event_ts = pending_tick
                    if seen_after is None or event_ts > seen_after:
                        yield self._format_sse("market", event, event_id), event_ts, event_id
                        seen_after = event_ts
                        current_last_id = event_id
                    pending_tick = None
                    last_flush = now

                if now - last_heartbeat >= self.heartbeat_interval:
                    yield self._heartbeat_frame(current_last_id), None, current_last_id
                    last_heartbeat = now

                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True),
                        timeout=0.1,
                    )
                except asyncio.TimeoutError:
                    continue

                if not message:
                    continue

                channel = message["channel"]
                data = json.loads(message["data"])

                if "kline" in channel:
                    event_id, event_ts = self._ensure_event_identity(
                        data,
                        prefix=f"{normalized_symbol}:{interval}",
                    )
                    if seen_after is not None and event_ts <= seen_after:
                        continue
                    data.setdefault("tf", data.get("tf") or interval)
                    data.setdefault("symbol", data.get("symbol") or normalized_symbol)
                    data.setdefault("displaySymbol", data.get("displaySymbol") or display_symbol)
                    pending_tick = (data, event_id, event_ts)
                    continue

                event_id, event_ts = self._ensure_event_identity(
                    data,
                    prefix=f"{normalized_symbol}:trade",
                )
                if pending_tick:
                    pending_event, pending_id, pending_ts = pending_tick
                    if seen_after is None or pending_ts > seen_after:
                        yield self._format_sse("market", pending_event, pending_id), pending_ts, pending_id
                        seen_after = pending_ts
                        current_last_id = pending_id
                    pending_tick = None
                    last_flush = time.monotonic()

                if seen_after is not None and event_ts <= seen_after:
                    continue

                current_last_id = event_id
                seen_after = event_ts
                yield self._format_sse("trade", data, event_id), event_ts, event_id
        finally:
            if pubsub:
                await pubsub.unsubscribe()
                await pubsub.close()
            if redis_client:
                await redis_client.close()

    async def _polling_market_stream(
        self,
        normalized_symbol: str,
        display_symbol: str,
        interval: str,
        resume_after: Optional[int],
        last_event_id: Optional[str],
    ) -> AsyncGenerator[Tuple[str, Optional[int], Optional[str]], None]:
        last_heartbeat = time.monotonic()
        seen_after = resume_after
        current_last_id = last_event_id
        use_testnet = os.getenv("BINANCE_TESTNET", "").lower() in {"1", "true", "yes"}
        base_url = os.getenv("BINANCE_TESTNET_REST_BASE", "https://testnet.binance.vision") if use_testnet else os.getenv("BINANCE_REST_BASE", "https://api.binance.com")

        while True:
            now = time.monotonic()
            if now - last_heartbeat >= self.heartbeat_interval:
                yield self._heartbeat_frame(current_last_id), None, current_last_id
                last_heartbeat = now

            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(
                        f"{base_url}/api/v3/klines",
                        params={"symbol": normalized_symbol, "interval": interval, "limit": 1},
                    )
                    response.raise_for_status()
                    klines = response.json()
            except Exception as exc:  # pragma: no cover - network hiccup
                logger.warning("market_polling_error", error=str(exc))
                await asyncio.sleep(self.coalesce_window)
                continue

            if not klines:
                await asyncio.sleep(self.coalesce_window)
                continue

            k = klines[0]
            event_ts = int(k[0])
            if seen_after is not None and event_ts <= seen_after:
                await asyncio.sleep(self.coalesce_window)
                continue

            event = {
                "type": "tick",
                "t": event_ts,
                "tf": interval,
                "symbol": normalized_symbol,
                "displaySymbol": display_symbol,
                "o": float(k[1]),
                "h": float(k[2]),
                "l": float(k[3]),
                "c": float(k[4]),
                "v": float(k[5]),
            }
            event_id = f"{normalized_symbol}:{interval}:{event_ts}"
            event["id"] = event_id
            current_last_id = event_id
            seen_after = event_ts
            yield self._format_sse("market", event, event_id), event_ts, event_id
            await asyncio.sleep(self.coalesce_window)

    async def _market_live_stream(
        self,
        normalized_symbol: str,
        display_symbol: str,
        interval: str,
        resume_after: Optional[int],
        last_event_id: Optional[str],
    ) -> AsyncGenerator[Tuple[str, Optional[int], Optional[str]], None]:
        if REDIS_AVAILABLE:
            async for frame in self._redis_market_stream(
                normalized_symbol,
                display_symbol,
                interval,
                resume_after,
                last_event_id,
            ):
                yield frame
        else:
            async for frame in self._polling_market_stream(
                normalized_symbol,
                display_symbol,
                interval,
                resume_after,
                last_event_id,
            ):
                yield frame

    async def market_event_stream(
        self,
        symbol: str,
        interval: str,
        last_event_id: Optional[str],
    ) -> AsyncGenerator[str, None]:
        normalized_symbol = _normalize_symbol(symbol)
        resume_after = _parse_event_timestamp(last_event_id)
        current_last_id = last_event_id

        yield self._retry_frame()

        async for frame, event_ts, event_id in self._backfill_market(symbol, interval, resume_after):
            resume_after = event_ts
            current_last_id = event_id
            yield frame

        async for frame, event_ts, event_id in self._market_live_stream(
            normalized_symbol,
            symbol,
            interval,
            resume_after,
            current_last_id,
        ):
            if event_ts is not None:
                resume_after = event_ts
                current_last_id = event_id
            yield frame

    # ------------------------------------------------------------------
    # Bot stream helpers
    # ------------------------------------------------------------------
    async def _redis_bot_stream(
        self,
        resume_after: Optional[int],
        last_event_id: Optional[str],
    ) -> AsyncGenerator[Tuple[str, Optional[int], Optional[str]], None]:
        redis_client = None
        pubsub = None
        last_heartbeat = time.monotonic()
        seen_after = resume_after
        current_last_id = last_event_id

        try:
            redis_client = redis.from_url(self.redis_url, decode_responses=True)
            pubsub = redis_client.pubsub()
            channels = ["bot:order", "bot:fill", "bot:signal", "bot:state", "bot:risk"]
            await pubsub.subscribe(*channels)
            logger.info("bot_sse_subscribed", extra={"channels": channels})

            while True:
                now = time.monotonic()
                if now - last_heartbeat >= self.heartbeat_interval:
                    yield self._heartbeat_frame(current_last_id), None, current_last_id
                    last_heartbeat = now

                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True),
                        timeout=0.1,
                    )
                except asyncio.TimeoutError:
                    continue

                if not message:
                    continue

                channel = message["channel"]
                event_type = channel.replace(":", ".")
                data = json.loads(message["data"])
                event_id, event_ts = self._ensure_event_identity(data, prefix=event_type)
                if seen_after is not None and event_ts <= seen_after:
                    continue
                seen_after = event_ts
                current_last_id = event_id
                yield self._format_sse(event_type, data, event_id), event_ts, event_id
        finally:
            if pubsub:
                await pubsub.unsubscribe()
                await pubsub.close()
            if redis_client:
                await redis_client.close()

    async def _polling_bot_stream(
        self,
        resume_after: Optional[int],
        last_event_id: Optional[str],
    ) -> AsyncGenerator[Tuple[str, Optional[int], Optional[str]], None]:
        last_heartbeat = time.monotonic()
        seen_after = resume_after
        current_last_id = last_event_id

        while True:
            now = time.monotonic()
            if now - last_heartbeat >= self.heartbeat_interval:
                yield self._heartbeat_frame(current_last_id), None, current_last_id
                last_heartbeat = now

            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(self._events_recent_url, params={"limit": 50})
                    response.raise_for_status()
                    payload = response.json()
            except Exception as exc:  # pragma: no cover - fallback only
                logger.warning("bot_polling_error", error=str(exc))
                await asyncio.sleep(self.coalesce_window)
                continue

            for event in payload.get("events", []):
                event_id, event_ts = self._ensure_event_identity(event, prefix="bot.event")
                if seen_after is not None and event_ts <= seen_after:
                    continue
                event_type = event.get("type") or f"bot.{event.get('kind', 'event')}"
                seen_after = event_ts
                current_last_id = event_id
                yield self._format_sse(event_type, event, event_id), event_ts, event_id

            await asyncio.sleep(self.coalesce_window)

    async def _bot_live_stream(
        self,
        resume_after: Optional[int],
        last_event_id: Optional[str],
    ) -> AsyncGenerator[Tuple[str, Optional[int], Optional[str]], None]:
        if REDIS_AVAILABLE:
            async for frame in self._redis_bot_stream(resume_after, last_event_id):
                yield frame
        else:
            async for frame in self._polling_bot_stream(resume_after, last_event_id):
                yield frame

    async def bot_event_stream(
        self,
        last_event_id: Optional[str],
    ) -> AsyncGenerator[str, None]:
        resume_after = _parse_event_timestamp(last_event_id)
        current_last_id = last_event_id

        yield self._retry_frame()

        async for frame, event_ts, event_id in self._bot_live_stream(resume_after, current_last_id):
            if event_ts is not None:
                resume_after = event_ts
                current_last_id = event_id
            yield frame


sse_producer = SSEProducer()


@router.get("/events/stream/market")
async def stream_market_events(
    request: Request,
    symbol: str = Query("BTC-USDT", description="Trading pair"),
    interval: str = Query("1m", description="Kline interval"),
    last_event_id: Optional[str] = Query(None, description="Resume from event id"),
):
    resume_token = request.headers.get("Last-Event-ID") or last_event_id
    return StreamingResponse(
        sse_producer.market_event_stream(symbol, interval, resume_token),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/events/stream/bot")
async def stream_bot_events(
    request: Request,
    last_event_id: Optional[str] = Query(None, description="Resume from event id"),
):
    resume_token = request.headers.get("Last-Event-ID") or last_event_id
    return StreamingResponse(
        sse_producer.bot_event_stream(resume_token),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
