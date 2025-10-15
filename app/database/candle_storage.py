"""
Candle storage service for infinite historical data.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
import logging

# Import will be from connection_sqlite via __init__
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .connection_sqlite import DatabaseManager
else:
    DatabaseManager = Any  # type: ignore

logger = logging.getLogger(__name__)


class CandleStorageService:
    """
    Service for storing and retrieving candles from persistent storage.
    Supports unlimited historical depth.
    """
    
    def __init__(self, db: DatabaseManager):
        """
        Initialize candle storage service.
        
        Args:
            db: Database manager instance
        """
        self.db = db
    
    async def store_candle(
        self,
        timestamp: datetime,
        symbol: str,
        timeframe: str,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: float,
        quote_volume: Optional[float] = None,
        trades: Optional[int] = None,
        source: str = 'binance'
    ) -> None:
        """
        Store a single candle to database.
        Uses REPLACE to update if candle already exists (SQLite).
        
        Args:
            timestamp: Candle timestamp
            symbol: Trading pair symbol
            timeframe: Timeframe (1m, 5m, 1h, etc.)
            open_price: Opening price
            high: High price
            low: Low price
            close: Close price
            volume: Base volume
            quote_volume: Quote volume (optional)
            trades: Number of trades (optional)
            source: Data source name
        """
        try:
            # Convert datetime to Unix timestamp for SQLite
            ts_unix = int(timestamp.timestamp()) if isinstance(timestamp, datetime) else timestamp
            
            await self.db.execute("""
                INSERT OR REPLACE INTO candles (
                    timestamp, symbol, timeframe, open, high, low, close, 
                    volume, quote_volume, trades, source
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, ts_unix, symbol, timeframe, 
                open_price, high, low, close, volume,
                quote_volume, trades, source
            )
        except Exception as e:
            logger.error(f"Failed to store candle: {e}")
            raise
    
    async def store_candles_bulk(self, candles: List[Dict[str, Any]]) -> int:
        """
        Store multiple candles in bulk for efficiency.
        
        Args:
            candles: List of candle dictionaries
        
        Returns:
            Number of candles stored
        """
        if not candles:
            return 0
        
        try:
            async with self.db.acquire() as conn:
                # Prepare records for bulk insert
                records = []
                for candle in candles:
                    ts = candle['timestamp']
                    ts_unix = int(ts.timestamp()) if isinstance(ts, datetime) else ts
                    
                    records.append((
                        ts_unix,
                        candle['symbol'],
                        candle['timeframe'],
                        candle['open'],
                        candle['high'],
                        candle['low'],
                        candle['close'],
                        candle['volume'],
                        candle.get('quote_volume'),
                        candle.get('trades'),
                        candle.get('source', 'binance')
                    ))
                
                # Use executemany for bulk insert (SQLite)
                await conn.executemany("""
                    INSERT OR REPLACE INTO candles (
                        timestamp, symbol, timeframe, open, high, low, close, 
                        volume, quote_volume, trades, source
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, records)
                await conn.commit()
                
                logger.info(f"Stored {len(candles)} candles to database")
                return len(candles)
        
        except Exception as e:
            logger.error(f"Failed to bulk store candles: {e}")
            # Fallback to individual inserts
            stored = 0
            for candle in candles:
                try:
                    await self.store_candle(**candle)
                    stored += 1
                except Exception as e2:
                    logger.error(f"Failed to store individual candle: {e2}")
            return stored
    
    async def get_candles(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Retrieve candles from database with flexible filtering.
        No limit on historical depth!
        
        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe (1m, 5m, 1h, etc.)
            start_time: Start timestamp (optional)
            end_time: End timestamp (optional)
            limit: Maximum number of candles (optional)
        
        Returns:
            List of candle dictionaries
        """
        query = """
            SELECT 
                timestamp, symbol, timeframe, 
                open, high, low, close, 
                volume, quote_volume, trades, source
            FROM candles
            WHERE symbol = ? AND timeframe = ?
        """
        params: List[Any] = [symbol, timeframe]
        
        if start_time:
            ts_unix = int(start_time.timestamp()) if isinstance(start_time, datetime) else start_time
            query += " AND timestamp >= ?"
            params.append(ts_unix)
        
        if end_time:
            ts_unix = int(end_time.timestamp()) if isinstance(end_time, datetime) else end_time
            query += " AND timestamp <= ?"
            params.append(ts_unix)
        
        query += " ORDER BY timestamp DESC"
        
        if limit:
            query += " LIMIT ?"
            params.append(limit)
        
        try:
            rows = await self.db.fetch(query, *params)
            
            candles = [
                {
                    'timestamp': datetime.fromtimestamp(row['timestamp']),
                    'symbol': row['symbol'],
                    'timeframe': row['timeframe'],
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': float(row['volume']),
                    'quote_volume': float(row['quote_volume']) if row['quote_volume'] else None,
                    'trades': row['trades'],
                    'source': row['source']
                }
                for row in rows
            ]
            
            return candles
        
        except Exception as e:
            logger.error(f"Failed to retrieve candles: {e}")
            return []
    
    async def get_candle_count(
        self,
        symbol: str,
        timeframe: str,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> int:
        """Get count of available candles for a symbol/timeframe."""
        query = "SELECT COUNT(*) FROM candles WHERE symbol = ? AND timeframe = ?"
        params: List[Any] = [symbol, timeframe]
        
        if start_time:
            ts_unix = int(start_time.timestamp()) if isinstance(start_time, datetime) else start_time
            query += " AND timestamp >= ?"
            params.append(ts_unix)
        
        if end_time:
            ts_unix = int(end_time.timestamp()) if isinstance(end_time, datetime) else end_time
            query += " AND timestamp <= ?"
            params.append(ts_unix)
        
        try:
            count = await self.db.fetchval(query, *params)
            return count or 0
        except Exception as e:
            logger.error(f"Failed to get candle count: {e}")
            return 0
    
    async def get_latest_candle(
        self,
        symbol: str,
        timeframe: str
    ) -> Optional[Dict[str, Any]]:
        """Get the most recent candle for a symbol/timeframe."""
        candles = await self.get_candles(symbol, timeframe, limit=1)
        return candles[0] if candles else None
    
    async def get_earliest_candle(
        self,
        symbol: str,
        timeframe: str
    ) -> Optional[Dict[str, Any]]:
        """Get the oldest candle for a symbol/timeframe."""
        query = """
            SELECT 
                timestamp, symbol, timeframe, 
                open, high, low, close, 
                volume, quote_volume, trades, source
            FROM candles
            WHERE symbol = ? AND timeframe = ?
            ORDER BY timestamp ASC
            LIMIT 1
        """
        
        try:
            row = await self.db.fetchrow(query, symbol, timeframe)
            if not row:
                return None
            
            return {
                'timestamp': datetime.fromtimestamp(row['timestamp']),
                'symbol': row['symbol'],
                'timeframe': row['timeframe'],
                'open': float(row['open']),
                'high': float(row['high']),
                'low': float(row['low']),
                'close': float(row['close']),
                'volume': float(row['volume']),
                'quote_volume': float(row['quote_volume']) if row['quote_volume'] else None,
                'trades': row['trades'],
                'source': row['source']
            }
        except Exception as e:
            logger.error(f"Failed to get earliest candle: {e}")
            return None
    
    async def delete_old_candles(
        self,
        symbol: str,
        timeframe: str,
        older_than: datetime
    ) -> int:
        """
        Delete candles older than specified date.
        Use for data retention policies if needed.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Timeframe
            older_than: Delete candles before this timestamp
        
        Returns:
            Number of candles deleted
        """
        try:
            ts_unix = int(older_than.timestamp()) if isinstance(older_than, datetime) else older_than
            
            # Get count before delete
            count = await self.db.fetchval("""
                SELECT COUNT(*) FROM candles
                WHERE symbol = ? 
                  AND timeframe = ? 
                  AND timestamp < ?
            """, symbol, timeframe, ts_unix)
            
            await self.db.execute("""
                DELETE FROM candles
                WHERE symbol = ? 
                  AND timeframe = ? 
                  AND timestamp < ?
            """, symbol, timeframe, ts_unix)
            
            deleted = count or 0
            logger.info(f"Deleted {deleted} old candles for {symbol}/{timeframe}")
            return deleted
        
        except Exception as e:
            logger.error(f"Failed to delete old candles: {e}")
            return 0
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get statistics about candle storage."""
        try:
            stats = await self.db.fetchrow("""
                SELECT 
                    COUNT(*) as total_candles,
                    COUNT(DISTINCT symbol) as unique_symbols,
                    COUNT(DISTINCT timeframe) as unique_timeframes,
                    MIN(timestamp) as earliest_candle,
                    MAX(timestamp) as latest_candle
                FROM candles
            """)
            
            return {
                'total_candles': stats['total_candles'] if stats else 0,
                'unique_symbols': stats['unique_symbols'] if stats else 0,
                'unique_timeframes': stats['unique_timeframes'] if stats else 0,
                'earliest_candle': datetime.fromtimestamp(stats['earliest_candle']) if stats and stats['earliest_candle'] else None,
                'latest_candle': datetime.fromtimestamp(stats['latest_candle']) if stats and stats['latest_candle'] else None,
                'table_size': 'N/A (SQLite)'
            }
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {}
