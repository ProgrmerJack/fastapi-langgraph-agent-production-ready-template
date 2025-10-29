"""Market data API router for providing candles and trade data to dashboard."""

import hashlib
import json
import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Query, Depends, Header, Response
from pydantic import BaseModel
from prometheus_client import Counter, Gauge

from app.core.logging import logger
from app.database import DatabaseManager, CandleStorageService

router = APIRouter()

# Simple in-memory cache for candles with ETag support
_candle_cache: Dict[str, tuple[List[Dict[str, Any]], float, str]] = {}  # key -> (data, timestamp, etag)
_CACHE_TTL = 30.0  # 30 seconds

# Prometheus metrics for ETag caching
_etag_cache_hits = Counter("history_cache_hits_total", "Number of 304 Not Modified responses")
_etag_cache_misses = Counter("history_cache_misses_total", "Number of cache misses (full data served)")
_etag_cache_hit_ratio = Gauge("history_304_ratio", "Ratio of 304 responses to total requests")

# Database manager for infinite storage
_db_manager: Optional[DatabaseManager] = None
_candle_storage: Optional[CandleStorageService] = None


class CandleResponse(BaseModel):
    """Response model for candle data."""

    candles: List[Dict[str, Any]]
    source: str
    symbol: str
    count: int


def _get_binance_base_url() -> str:
    """Get Binance API base URL based on testnet setting."""
    use_testnet = os.getenv("BINANCE_TESTNET", "").lower() in ("true", "1", "yes")
    
    if use_testnet:
        return os.getenv("BINANCE_TESTNET_REST_BASE", "https://testnet.binance.vision")
    return "https://api.binance.com"


def _normalize_symbol(symbol: str) -> str:
    """Normalize symbol to Binance format (e.g., BTC-USDT -> BTCUSDT)."""
    return symbol.replace("-", "").replace("/", "").replace("_", "").upper()



_INTERVAL_MS: Dict[str, int] = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
}


def _interval_to_ms(label: str) -> int:
    """Return the approximate interval length in milliseconds."""
    return _INTERVAL_MS.get(str(label).lower(), 60_000)


async def get_candle_storage() -> CandleStorageService:
    """Get or initialize the candle storage service."""
    global _db_manager, _candle_storage
    
    if _db_manager is None:
        _db_manager = DatabaseManager("data/hypertrader.db")
        await _db_manager.connect()
        await _db_manager.initialize_schema()
        _candle_storage = CandleStorageService(_db_manager)
        logger.info("Initialized candle storage service with infinite history")
    
    return _candle_storage



@router.get("/market/candles", response_model=None)
async def get_candles(
    response: Response,
    symbol: str = Query("BTC-USDT", description="Trading pair symbol"),
    tf: str = Query("1m", description="Timeframe (1m, 5m, 15m, 1h, etc.)"),
    limit: Optional[int] = Query(
        500,
        ge=0,
        le=50000,
        description="Number of candles to return (0 = all available)",
    ),
    start_time: Optional[int] = Query(None, description="Start timestamp (ms) for historical data"),
    end_time: Optional[int] = Query(None, description="End timestamp (ms) for historical data"),
    if_none_match: Optional[str] = Header(None),
    storage: CandleStorageService = Depends(get_candle_storage),
):
    """Return historical candles from storage or Binance with smart pagination."""

    if limit in (None, 0):
        limit_value: Optional[int] = None
    else:
        limit_value = max(1, int(limit))

    try:
        start_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc) if start_time else None
        end_dt = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc) if end_time else None

        db_candles = await storage.get_candles(
            symbol=symbol,
            timeframe=tf,
            start_time=start_dt,
            end_time=end_dt,
            limit=limit_value,
        )

        if db_candles:
            candles = [
                {
                    "t": int(c["timestamp"].timestamp() * 1000),
                    "o": c["open"],
                    "h": c["high"],
                    "l": c["low"],
                    "c": c["close"],
                    "v": c["volume"],
                }
                for c in reversed(db_candles)
            ]

            logger.info(
                "Served %s candles from database", len(candles),
                extra={
                    "symbol": symbol,
                    "tf": tf,
                    "limit": limit_value if limit_value is not None else "all",
                    "start": start_time,
                    "end": end_time,
                },
            )
            return CandleResponse(
                candles=candles,
                source="database",
                symbol=symbol,
                count=len(candles),
            )
    except Exception as exc:
        logger.warning(f"Database query failed, falling back to Binance: {exc}")

    cache_limit = "all" if limit_value is None else str(limit_value)
    cache_start = str(start_time) if start_time is not None else "_"
    cache_end = str(end_time) if end_time is not None else "_"
    cache_key = f"{symbol}:{tf}:{cache_limit}:{cache_start}:{cache_end}"
    now = datetime.now(timezone.utc).timestamp()

    def calculate_etag(candle_data: List[Dict[str, Any]]) -> str:
        data_str = json.dumps(candle_data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()

    def update_cache_metrics() -> None:
        total_hits = _etag_cache_hits._value._value
        total_misses = _etag_cache_misses._value._value
        total = total_hits + total_misses
        if total > 0:
            _etag_cache_hit_ratio.set(total_hits / total)

    allow_cache = (
        limit_value is not None
        and limit_value <= 10000
        and start_time is None
        and end_time is None
    )

    if allow_cache and cache_key in _candle_cache:
        cached_data, cached_time, cached_etag = _candle_cache[cache_key]
        if now - cached_time < _CACHE_TTL:
            if if_none_match and if_none_match == cached_etag:
                logger.debug("ETag match for %s - returning 304", cache_key)
                _etag_cache_hits.inc()
                update_cache_metrics()
                return Response(status_code=304)

            logger.debug("Serving candles from cache: %s", cache_key)
            _etag_cache_misses.inc()
            update_cache_metrics()
            response.headers["ETag"] = cached_etag
            response.headers["Cache-Control"] = "max-age=60"
            return CandleResponse(
                candles=cached_data,
                source="cache",
                symbol=symbol,
                count=len(cached_data),
            )

    binance_symbol = _normalize_symbol(symbol)
    binance_base = _get_binance_base_url()
    step_ms = max(1, _interval_to_ms(tf))
    iterations = 0
    max_iterations = 24
    next_end = end_time
    include_start = start_time
    remaining = limit_value
    fetched_candles: List[Dict[str, Any]] = []

    while iterations < max_iterations:
        batch_limit = remaining if remaining is not None else 1000
        batch_limit = max(1, min(1000, batch_limit))
        fetched_batch = None

        for endpoint in ("uiKlines", "klines"):
            url = f"{binance_base}/api/v3/{endpoint}"
            params: Dict[str, Any] = {
                "symbol": binance_symbol,
                "interval": tf,
                "limit": batch_limit,
            }
            if include_start is not None and iterations == 0:
                params["startTime"] = include_start
            if next_end is not None:
                params["endTime"] = next_end

            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(url, params=params)
                    response.raise_for_status()
                    raw_candles = response.json()
            except Exception:
                continue

            if not isinstance(raw_candles, list) or not raw_candles:
                continue

            fetched_batch = raw_candles
            break

        if fetched_batch is None:
            break

        for kline in fetched_batch:
            try:
                open_price = float(kline[1])
                high = float(kline[2])
                low = float(kline[3])
                close = float(kline[4])
                volume = float(kline[5])
                open_time_ms = int(float(kline[0]))
                close_time_ms = int(float(kline[6])) if len(kline) > 6 else open_time_ms
                quote_volume = float(kline[7]) if len(kline) > 7 else 0.0
                trade_count = int(float(kline[8])) if len(kline) > 8 else 0
            except (ValueError, TypeError, IndexError):
                continue

            fetched_candles.append(
                {
                    "t": open_time_ms,
                    "o": open_price,
                    "h": high,
                    "l": low,
                    "c": close,
                    "v": volume,
                    "close_time": close_time_ms,
                    "quote_volume": quote_volume,
                    "trades": trade_count,
                }
            )

        if remaining is not None:
            remaining -= len(fetched_batch)
            if remaining <= 0:
                break

        earliest_open = int(float(fetched_batch[0][0]))
        next_end = earliest_open - step_ms
        if next_end <= 0:
            break

        if len(fetched_batch) < batch_limit:
            break

        include_start = None
        iterations += 1

    if fetched_candles:
        dedup: Dict[int, Dict[str, Any]] = {}
        for candle in fetched_candles:
            dedup[candle["t"]] = candle
        ordered_times = sorted(dedup)
        candles = [dedup[t] for t in ordered_times]

        etag = calculate_etag(candles)
        if allow_cache:
            _candle_cache[cache_key] = (candles, now, etag)

        try:
            candles_to_store = [
                {
                    "timestamp": datetime.fromtimestamp(c["t"] / 1000, tz=timezone.utc),
                    "symbol": symbol,
                    "timeframe": tf,
                    "open": c["o"],
                    "high": c["h"],
                    "low": c["l"],
                    "close": c["c"],
                    "volume": c["v"],
                }
                for c in candles
            ]
            stored_count = await storage.store_candles_bulk(candles_to_store)
            logger.debug("Stored %s new candles in database", stored_count)
        except Exception as store_exc:
            logger.warning(f"Failed to store candles in database: {store_exc}")

        _etag_cache_misses.inc()
        update_cache_metrics()
        if allow_cache:
            response.headers["ETag"] = etag
            response.headers["Cache-Control"] = "max-age=60"

        return CandleResponse(
            candles=candles,
            source="binance_rest",
            symbol=symbol,
            count=len(candles),
        )

    logger.error(
        "Failed to fetch candles from all Binance endpoints",
        extra={"symbol": binance_symbol, "tf": tf},
    )

    return CandleResponse(
        candles=[],
        source="error",
        symbol=symbol,
        count=0,
    )
@router.get("/market/time")
async def get_exchange_time() -> Dict[str, Any]:
    """Get current exchange server time from Binance."""
    binance_base = _get_binance_base_url()
    url = f"{binance_base}/api/v3/time"
    
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            
            return {
                "serverTime": data.get("serverTime"),
                "iso": datetime.fromtimestamp(
                    data.get("serverTime", 0) / 1000, tz=timezone.utc
                ).isoformat(),
            }
    except Exception as exc:
        logger.error(f"Failed to get exchange time: {exc}")
        return {"serverTime": None, "iso": None, "error": str(exc)}


@router.get("/market/health")
async def market_health() -> Dict[str, Any]:
    """Health check for market data endpoints."""
    return {
        "status": "ok",
        "cache_entries": len(_candle_cache),
        "binance_base": _get_binance_base_url(),
        "cache_ttl": _CACHE_TTL,
    }

