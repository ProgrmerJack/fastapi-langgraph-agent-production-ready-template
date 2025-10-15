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
    limit: int = Query(500, ge=1, le=10000, description="Number of candles to return"),
    start_time: Optional[int] = Query(None, description="Start timestamp (ms) for historical data"),
    end_time: Optional[int] = Query(None, description="End timestamp (ms) for historical data"),
    if_none_match: Optional[str] = Header(None),
    storage: CandleStorageService = Depends(get_candle_storage),
):
    """
    Get historical candle (OHLCV) data with infinite storage.
    
    This endpoint now uses database storage for unlimited history.
    First checks database, then fetches from Binance if needed.
    All fetched candles are automatically stored for future queries.
    
    Args:
        symbol: Trading pair (e.g., BTC-USDT, ETH-USDT)
        tf: Timeframe/interval (1m, 5m, 15m, 1h, 4h, 1d)
        limit: Number of candles (1-1000)
        start_time: Optional start timestamp for historical queries
        end_time: Optional end timestamp for historical queries
        storage: Database storage service (auto-injected)
    
    Returns:
        CandleResponse with candle data
    """
    # Always try database first for historical data (provides full depth!)
    try:
        # Convert timestamps if provided
        start_dt = datetime.fromtimestamp(start_time / 1000, tz=timezone.utc) if start_time else None
        end_dt = datetime.fromtimestamp(end_time / 1000, tz=timezone.utc) if end_time else None
        
        db_candles = await storage.get_candles(
            symbol=symbol,
            timeframe=tf,
            start_time=start_dt,
            end_time=end_dt,
            limit=limit
        )
        
        if db_candles:
            # Convert database format to API format
            # Database returns DESC order (newest first), reverse for chart display
            candles = [
                {
                    "t": int(c["timestamp"].timestamp() * 1000),
                    "o": c["open"],
                    "h": c["high"],
                    "l": c["low"],
                    "c": c["close"],
                    "v": c["volume"],
                }
                for c in reversed(db_candles)  # Reverse to oldest->newest for charts
            ]
            
            logger.info(f"Served {len(candles)} candles from database", extra={"symbol": symbol, "tf": tf, "limit": limit})
            return CandleResponse(
                candles=candles,
                source="database",
                symbol=symbol,
                count=len(candles),
            )
    except Exception as e:
        logger.warning(f"Database query failed, falling back to Binance: {e}")
    
    cache_key = f"{symbol}:{tf}:{limit}"
    now = datetime.now(timezone.utc).timestamp()
    
    # Helper function to calculate ETag
    def calculate_etag(candle_data: List[Dict[str, Any]]) -> str:
        """Calculate ETag hash from candle data."""
        data_str = json.dumps(candle_data, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()
    
    # Helper function to update metrics
    def update_cache_metrics():
        """Update Prometheus cache hit ratio."""
        total_hits = _etag_cache_hits._value._value
        total_misses = _etag_cache_misses._value._value
        total = total_hits + total_misses
        if total > 0:
            _etag_cache_hit_ratio.set(total_hits / total)
    
    # Check cache
    if cache_key in _candle_cache:
        cached_data, cached_time, cached_etag = _candle_cache[cache_key]
        if now - cached_time < _CACHE_TTL:
            # Check if client has current version (ETag match)
            if if_none_match and if_none_match == cached_etag:
                logger.debug(f"ETag match for {cache_key} - returning 304")
                _etag_cache_hits.inc()
                update_cache_metrics()
                return Response(status_code=304)  # Not Modified
            
            # Cache hit but ETag mismatch or no ETag provided
            logger.debug(f"Serving candles from cache: {cache_key}")
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
    
    # Fetch from Binance
    binance_symbol = _normalize_symbol(symbol)
    binance_base = _get_binance_base_url()
    
    # Try uiKlines first (better for charts), fall back to klines
    endpoints = ["uiKlines", "klines"]
    candles: List[Dict[str, Any]] = []
    
    for endpoint in endpoints:
        url = f"{binance_base}/api/v3/{endpoint}"
        params = {
            "symbol": binance_symbol,
            "interval": tf,
            "limit": limit,
        }
        
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                
                raw_candles = response.json()
                
                # Convert Binance kline format to chart-friendly OHLCV dict
                for kline in raw_candles:
                    try:
                        open_price = float(kline[1])
                        high = float(kline[2])
                        low = float(kline[3])
                        close = float(kline[4])
                        volume = float(kline[5])
                    except (ValueError, TypeError, IndexError):
                        continue

                    timestamp_ms = int(kline[0])
                    close_time_ms = int(kline[6]) if len(kline) > 6 else timestamp_ms
                    quote_volume = float(kline[7]) if len(kline) > 7 else 0.0
                    trade_count = int(kline[8]) if len(kline) > 8 else 0

                    candles.append(
                        {
                            "t": timestamp_ms,
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
                
                # Success! Calculate ETag, cache and return
                etag = calculate_etag(candles)
                _candle_cache[cache_key] = (candles, now, etag)
                
                # Store in database for infinite history
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
                    logger.debug(f"Stored {stored_count} new candles in database")
                except Exception as e:
                    logger.warning(f"Failed to store candles in database: {e}")
                
                logger.info(
                    f"Fetched {len(candles)} candles from Binance",
                    extra={
                        "symbol": binance_symbol,
                        "interval": tf,
                        "endpoint": endpoint,
                        "source": binance_base,
                    },
                )
                
                # Set ETag and cache headers
                _etag_cache_misses.inc()
                update_cache_metrics()
                response.headers["ETag"] = etag
                response.headers["Cache-Control"] = "max-age=60"
                
                return CandleResponse(
                    candles=candles,
                    source=f"binance_{endpoint}",
                    symbol=symbol,
                    count=len(candles),
                )
        
        except httpx.HTTPStatusError as exc:
            logger.warning(
                f"Binance API error on {endpoint}: {exc.response.status_code}",
                extra={"symbol": binance_symbol, "endpoint": endpoint},
            )
            continue
        except Exception as exc:
            logger.warning(
                f"Failed to fetch from {endpoint}: {exc}",
                extra={"symbol": binance_symbol},
            )
            continue
    
    # All attempts failed
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

