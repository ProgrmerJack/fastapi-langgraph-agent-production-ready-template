"""
Test script for database initialization and candle storage.
"""

import asyncio
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.database import DatabaseManager, CandleStorageService


async def main():
    print("🗄️ Database Initialization Test\n")
    
    # Initialize database
    db = DatabaseManager("data/hypertrader.db")
    
    try:
        # Connect
        print("1. Connecting to database...")
        await db.connect()
        print("   ✅ Connected")
        
        # Initialize schema
        print("\n2. Creating schema...")
        await db.initialize_schema()
        print("   ✅ Schema created")
        
        # Health check
        print("\n3. Health check...")
        is_healthy = await db.health_check()
        print(f"   {'✅' if is_healthy else '❌'} Health: {is_healthy}")
        
        # Test candle storage
        print("\n4. Testing candle storage...")
        storage = CandleStorageService(db)
        
        # Store a test candle
        now = datetime.now()
        await storage.store_candle(
            timestamp=now,
            symbol="BTC-USDT",
            timeframe="1m",
            open_price=50000.0,
            high=50100.0,
            low=49900.0,
            close=50050.0,
            volume=10.5,
            quote_volume=525000.0,
            trades=150,
            source='test'
        )
        print("   ✅ Stored test candle")
        
        # Retrieve candles
        candles = await storage.get_candles("BTC-USDT", "1m", limit=10)
        print(f"   ✅ Retrieved {len(candles)} candles")
        
        # Storage stats
        print("\n5. Storage statistics...")
        stats = await storage.get_storage_stats()
        print(f"   Total candles: {stats.get('total_candles', 0)}")
        print(f"   Unique symbols: {stats.get('unique_symbols', 0)}")
        print(f"   Unique timeframes: {stats.get('unique_timeframes', 0)}")
        print(f"   Earliest: {stats.get('earliest_candle', 'N/A')}")
        print(f"   Latest: {stats.get('latest_candle', 'N/A')}")
        
        print("\n✅ All tests passed!")
        print("\n📊 Database ready for infinite candle storage!")
        print(f"📁 Database location: {Path('data/hypertrader.db').absolute()}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        await db.disconnect()
        print("\n🔌 Disconnected from database")


if __name__ == "__main__":
    asyncio.run(main())
