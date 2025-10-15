"""
Backfill historical candle data from Binance.
Run this once to populate database with historical data.
"""

import asyncio
import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

import httpx
from dotenv import load_dotenv

from app.database import DatabaseManager, CandleStorageService

load_dotenv()


def get_binance_base_url() -> str:
    """Get Binance API base URL."""
    use_testnet = os.getenv("BINANCE_TESTNET", "").lower() in ("true", "1", "yes")
    if use_testnet:
        return os.getenv("BINANCE_TESTNET_REST_BASE", "https://testnet.binance.vision")
    return "https://api.binance.com"


def normalize_symbol(symbol: str) -> str:
    """Normalize symbol to Binance format."""
    return symbol.replace("-", "").replace("/", "").replace("_", "").upper()


async def fetch_candles_batch(
    symbol: str,
    interval: str,
    start_time: int,
    end_time: int,
    limit: int = 1000
) -> List[Dict[str, Any]]:
    """Fetch a batch of candles from Binance."""
    binance_base = get_binance_base_url()
    binance_symbol = normalize_symbol(symbol)
    url = f"{binance_base}/api/v3/klines"
    
    params = {
        "symbol": binance_symbol,
        "interval": interval,
        "startTime": start_time,
        "endTime": end_time,
        "limit": limit,
    }
    
    candles = []
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            raw_candles = response.json()
            
            for kline in raw_candles:
                timestamp_ms = int(kline[0])
                candles.append({
                    "timestamp": datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc),
                    "symbol": symbol,
                    "timeframe": interval,
                    "open": float(kline[1]),
                    "high": float(kline[2]),
                    "low": float(kline[3]),
                    "close": float(kline[4]),
                    "volume": float(kline[5]),
                })
    except Exception as e:
        print(f"❌ Error fetching batch: {e}")
    
    return candles


async def backfill_symbol(
    storage: CandleStorageService,
    symbol: str,
    interval: str,
    days_back: int = 30
):
    """Backfill historical data for a symbol."""
    print(f"\n📊 Backfilling {symbol} {interval} (last {days_back} days)...")
    
    # Calculate time range
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=days_back)
    
    # Binance limit is 1000 candles per request
    # For 1m interval: 1000 minutes = ~16.67 hours per batch
    # For 5m interval: 5000 minutes = ~3.47 days per batch
    # For 1h interval: 1000 hours = ~41.67 days per batch
    
    interval_minutes = {
        "1m": 1,
        "5m": 5,
        "15m": 15,
        "1h": 60,
        "4h": 240,
        "1d": 1440,
    }.get(interval, 1)
    
    batch_duration = timedelta(minutes=1000 * interval_minutes)
    
    current_start = start_time
    total_stored = 0
    batch_num = 0
    
    while current_start < end_time:
        batch_num += 1
        current_end = min(current_start + batch_duration, end_time)
        
        print(f"  Batch {batch_num}: {current_start.strftime('%Y-%m-%d %H:%M')} to {current_end.strftime('%Y-%m-%d %H:%M')}")
        
        # Fetch batch
        candles = await fetch_candles_batch(
            symbol=symbol,
            interval=interval,
            start_time=int(current_start.timestamp() * 1000),
            end_time=int(current_end.timestamp() * 1000),
        )
        
        if candles:
            # Store in database
            stored = await storage.store_candles_bulk(candles)
            total_stored += stored
            print(f"    ✅ Stored {stored} new candles (total: {total_stored})")
        else:
            print(f"    ⚠️  No candles received")
        
        # Move to next batch
        current_start = current_end
        
        # Rate limiting - be nice to Binance
        await asyncio.sleep(0.2)
    
    print(f"✅ Completed {symbol} {interval}: {total_stored} candles stored")
    return total_stored


async def main():
    """Main backfill routine."""
    print("🚀 Starting Historical Candle Backfill")
    print("=" * 60)
    
    # Initialize database
    db = DatabaseManager("data/hypertrader.db")
    await db.connect()
    await db.initialize_schema()
    storage = CandleStorageService(db)
    
    print("✅ Database connected")
    
    # Configuration: which symbols and timeframes to backfill
    symbols = ["BTC-USDT", "ETH-USDT"]
    intervals = ["1m", "5m", "1h"]
    days_back = 30  # Last 30 days
    
    print(f"\nConfiguration:")
    print(f"  Symbols: {', '.join(symbols)}")
    print(f"  Intervals: {', '.join(intervals)}")
    print(f"  Days back: {days_back}")
    
    total_candles = 0
    
    # Backfill each symbol/interval combination
    for symbol in symbols:
        for interval in intervals:
            try:
                count = await backfill_symbol(storage, symbol, interval, days_back)
                total_candles += count
            except Exception as e:
                print(f"❌ Error backfilling {symbol} {interval}: {e}")
    
    # Show final statistics
    print("\n" + "=" * 60)
    print("📊 Backfill Complete!")
    print(f"   Total candles stored: {total_candles:,}")
    
    stats = await storage.get_storage_stats()
    print(f"\n📈 Database Statistics:")
    print(f"   Total candles: {stats.get('total_candles', 0):,}")
    print(f"   Unique symbols: {stats.get('unique_symbols', 0)}")
    print(f"   Unique timeframes: {stats.get('unique_timeframes', 0)}")
    if stats.get('earliest_candle'):
        print(f"   Earliest: {stats['earliest_candle']}")
    if stats.get('latest_candle'):
        print(f"   Latest: {stats['latest_candle']}")
    
    await db.disconnect()
    print("\n✅ Done!")


if __name__ == "__main__":
    asyncio.run(main())
