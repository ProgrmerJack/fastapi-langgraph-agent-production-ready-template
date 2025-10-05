"""Market data API router for providing candles and trade data to dashboard."""

import os
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Query
from pydantic import BaseModel

from app.core.logging import logger

router = APIRouter()

# Simple in-memory cache for candles
_candle_cache: Dict[str, tuple[List[Dict[str, Any]], float]] = {}
_CACHE_TTL = 30.0  # 30 seconds


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


@router.get("/market/candles")
async def get_candles(
    symbol: str = Query("BTC-USDT", description="Trading pair symbol"),
    tf: str = Query("1m", description="Timeframe (1m, 5m, 15m, 1h, etc.)"),
    limit: int = Query(100, ge=1, le=1000, description="Number of candles to return"),
) -> CandleResponse:
    """
    Get historical candle (OHLCV) data from Binance.
    
    This endpoint proxies to Binance API and caches results for 30 seconds.
    Used by the dashboard to display price charts.
    
    Args:
        symbol: Trading pair (e.g., BTC-USDT, ETH-USDT)
        tf: Timeframe/interval (1m, 5m, 15m, 1h, 4h, 1d)
        limit: Number of candles (1-1000)
    
    Returns:
        CandleResponse with candle data
    """
    cache_key = f"{symbol}:{tf}:{limit}"
    now = datetime.now(timezone.utc).timestamp()
    
    # Check cache
    if cache_key in _candle_cache:
        cached_data, cached_time = _candle_cache[cache_key]
        if now - cached_time < _CACHE_TTL:
            logger.debug(f"Serving candles from cache: {cache_key}")
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
    candles = []
    
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
                
                # Convert Binance kline format to standard OHLCV dict
                for kline in raw_candles:
                    candles.append({
                        "time": kline[0] / 1000,  # Convert ms to seconds
                        "open": float(kline[1]),
                        "high": float(kline[2]),
                        "low": float(kline[3]),
                        "close": float(kline[4]),
                        "volume": float(kline[5]),
                        "close_time": kline[6] / 1000,
                        "quote_volume": float(kline[7]) if len(kline) > 7 else 0.0,
                        "trades": int(kline[8]) if len(kline) > 8 else 0,
                    })
                
                # Success! Cache and return
                _candle_cache[cache_key] = (candles, now)
                
                logger.info(
                    f"Fetched {len(candles)} candles from Binance",
                    extra={
                        "symbol": binance_symbol,
                        "interval": tf,
                        "endpoint": endpoint,
                        "source": binance_base,
                    },
                )
                
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

