"""
Server-Sent Events (SSE) endpoints for real-time market and bot data streaming.
Implements dual-channel SSE: market data (klines, trades) + bot events (orders, signals, PnL, risk).
"""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, Optional
import httpx
from fastapi import APIRouter, Query
from fastapi.responses import StreamingResponse
from app.core.logging import logger

router = APIRouter()

# Redis integration (optional - graceful fallback)
try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    logger.warning("redis.asyncio not available - SSE will use polling")


class SSEProducer:
    """Produces Server-Sent Events for real-time updates."""

    def __init__(self):
        self.heartbeat_interval = 15  # seconds
        self.redis_url = "redis://localhost:6379"
        self.binance_ws_url = "wss://stream.binance.com:9443/ws"
        self.testnet = False  # Will be set from env

    async def market_event_stream(
        self,
        symbol: str = "BTCUSDT",
        interval: str = "1m"
    ) -> AsyncGenerator[str, None]:
        """
        Market data SSE stream: klines + trades.

        Events:
        - event: market -> kline data
        - event: trade -> trade ticks
        - event: ping -> heartbeat
        """
        last_heartbeat = time.time()

        # Normalize symbol (BTC-USDT -> BTCUSDT)
        symbol = symbol.replace("-", "").upper()

        try:
            if REDIS_AVAILABLE:
                # Try Redis Pub/Sub first (bot publishes here)
                async for event_str in self._redis_market_stream(symbol, interval):
                    yield event_str
            else:
                # Fallback: poll Binance REST API for klines
                async for event_str in self._polling_market_stream(symbol, interval):
                    yield event_str

        except asyncio.CancelledError:
            logger.info(f"Market SSE stream cancelled for {symbol}")
        except Exception as e:
            logger.error(f"Market SSE error: {e}")
            yield f": error: {e}\n\n"

    async def bot_event_stream(self) -> AsyncGenerator[str, None]:
        """
        Bot events SSE stream: orders, fills, signals, PnL, risk.

        Events:
        - event: bot.order -> new/updated orders
        - event: bot.fill -> order fills
        - event: bot.signal -> strategy signals
        - event: bot.state -> position + PnL + risk snapshot
        - event: ping -> heartbeat
        """
        last_heartbeat = time.time()

        try:
            if REDIS_AVAILABLE:
                async for event_str in self._redis_bot_stream():
                    yield event_str
            else:
                # Fallback: poll agent backend events API
                async for event_str in self._polling_bot_stream():
                    yield event_str

        except asyncio.CancelledError:
            logger.info("Bot SSE stream cancelled")
        except Exception as e:
            logger.error(f"Bot SSE error: {e}")
            yield f": error: {e}\n\n"

    async def _redis_market_stream(
        self,
        symbol: str,
        interval: str
    ) -> AsyncGenerator[str, None]:
        """Redis Pub/Sub for market data."""
        redis_client = None
        pubsub = None
        last_heartbeat = time.time()

        try:
            redis_client = redis.from_url(self.redis_url, decode_responses=True)
            pubsub = redis_client.pubsub()

            # Subscribe to market channels
            channels = [
                f"market:kline:{symbol}:{interval}",
                f"market:trade:{symbol}"
            ]
            await pubsub.subscribe(*channels)
            logger.info(f"📡 Market SSE subscribed to Redis: {channels}")

            while True:
                # Heartbeat
                now = time.time()
                if now - last_heartbeat >= self.heartbeat_interval:
                    yield f"event: ping\ndata: {{\"t\":{int(now * 1000)}}}\n\n"
                    last_heartbeat = now

                # Get message
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True),
                        timeout=0.1
                    )

                    if message and message["type"] == "message":
                        data = json.loads(message["data"])
                        event_type = "market" if "kline" in message["channel"] else "trade"
                        yield f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

                except asyncio.TimeoutError:
                    pass

                await asyncio.sleep(0.01)

        finally:
            if pubsub:
                await pubsub.unsubscribe()
                await pubsub.close()
            if redis_client:
                await redis_client.close()

    async def _redis_bot_stream(self) -> AsyncGenerator[str, None]:
        """Redis Pub/Sub for bot events."""
        redis_client = None
        pubsub = None
        last_heartbeat = time.time()

        try:
            redis_client = redis.from_url(self.redis_url, decode_responses=True)
            pubsub = redis_client.pubsub()

            # Subscribe to bot channels
            channels = [
                "bot:order",
                "bot:fill",
                "bot:signal",
                "bot:state"
            ]
            await pubsub.subscribe(*channels)
            logger.info(f"📡 Bot SSE subscribed to Redis: {channels}")

            while True:
                # Heartbeat
                now = time.time()
                if now - last_heartbeat >= self.heartbeat_interval:
                    yield f"event: ping\ndata: {{\"t\":{int(now * 1000)}}}\n\n"
                    last_heartbeat = now

                # Get message
                try:
                    message = await asyncio.wait_for(
                        pubsub.get_message(ignore_subscribe_messages=True),
                        timeout=0.1
                    )

                    if message and message["type"] == "message":
                        data = json.loads(message["data"])
                        # Event type from channel name (bot:order -> bot.order)
                        event_type = message["channel"].replace(":", ".")
                        yield f"event: {event_type}\ndata: {json.dumps(data)}\n\n"

                except asyncio.TimeoutError:
                    pass

                await asyncio.sleep(0.01)

        finally:
            if pubsub:
                await pubsub.unsubscribe()
                await pubsub.close()
            if redis_client:
                await redis_client.close()

    async def _polling_market_stream(
        self,
        symbol: str,
        interval: str
    ) -> AsyncGenerator[str, None]:
        """Polling fallback for market data (when Redis unavailable)."""
        last_heartbeat = time.time()
        last_kline_time = 0

        # Get Binance REST base URL
        import os
        use_testnet = os.getenv("BINANCE_TESTNET", "").lower() in ("true", "1")
        base_url = "https://testnet.binance.vision" if use_testnet else "https://api.binance.com"

        while True:
            now = time.time()

            # Heartbeat
            if now - last_heartbeat >= self.heartbeat_interval:
                yield f"event: ping\ndata: {{\"t\":{int(now * 1000)}}}\n\n"
                last_heartbeat = now

            # Poll for new klines every 60 seconds (1m interval)
            if now - last_kline_time >= 60:
                try:
                    async with httpx.AsyncClient(timeout=5.0) as client:
                        response = await client.get(
                            f"{base_url}/api/v3/klines",
                            params={"symbol": symbol, "interval": interval, "limit": 1}
                        )
                        response.raise_for_status()
                        klines = response.json()

                        if klines:
                            k = klines[0]
                            event_data = {
                                "t": int(k[0]),
                                "o": float(k[1]),
                                "h": float(k[2]),
                                "l": float(k[3]),
                                "c": float(k[4]),
                                "v": float(k[5])
                            }
                            yield f"event: market\ndata: {json.dumps(event_data)}\n\n"
                            last_kline_time = now

                except Exception as e:
                    logger.warning(f"Market polling error: {e}")

            await asyncio.sleep(1.0)

    async def _polling_bot_stream(self) -> AsyncGenerator[str, None]:
        """Polling fallback for bot events (when Redis unavailable)."""
        last_heartbeat = time.time()
        last_event_id = 0

        while True:
            now = time.time()

            # Heartbeat
            if now - last_heartbeat >= self.heartbeat_interval:
                yield f"event: ping\ndata: {{\"t\":{int(now * 1000)}}}\n\n"
                last_heartbeat = now

            # Poll /events/recent every 2 seconds
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    response = await client.get(
                        "http://127.0.0.1:8601/events/recent",
                        params={"limit": 10}
                    )
                    response.raise_for_status()
                    events = response.json().get("events", [])

                    for event in events:
                        event_id = event.get("id", 0)
                        if event_id > last_event_id:
                            # Map event kind to SSE event type
                            kind = event.get("kind", "unknown")
                            event_type = f"bot.{kind}"
                            yield f"event: {event_type}\ndata: {json.dumps(event)}\n\n"
                            last_event_id = event_id

            except Exception as e:
                logger.warning(f"Bot polling error: {e}")

            await asyncio.sleep(2.0)


sse_producer = SSEProducer()


@router.get("/events/stream/market")
async def stream_market_events(
    symbol: str = Query("BTC-USDT", description="Trading pair"),
    interval: str = Query("1m", description="Kline interval")
):
    """
    SSE endpoint for live market data.

    Streams:
    - Kline/candle updates (event: market)
    - Trade ticks (event: trade)
    - Heartbeats (event: ping)
    """
    return StreamingResponse(
        sse_producer.market_event_stream(symbol, interval),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


@router.get("/events/stream/bot")
async def stream_bot_events():
    """
    SSE endpoint for live bot events.

    Streams:
    - Orders (event: bot.order)
    - Fills (event: bot.fill)
    - Signals (event: bot.signal)
    - State snapshots (event: bot.state)
    - Heartbeats (event: ping)
    """
    return StreamingResponse(
        sse_producer.bot_event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
