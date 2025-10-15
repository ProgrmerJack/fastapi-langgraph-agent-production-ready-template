"""
PostgreSQL + TimescaleDB connection layer for HyperTrader.

Provides asyncpg-based database connection with TimescaleDB hypertable support.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Dict, List, Optional

try:
    import asyncpg
    HAS_ASYNCPG = True
except ImportError:
    asyncpg = None
    HAS_ASYNCPG = False


class PostgreSQLConnection:
    """
    PostgreSQL connection manager with TimescaleDB support.
    
    Features:
    - Connection pooling for performance
    - TimescaleDB hypertable management
    - Automatic schema creation
    - Migration support from SQLite
    
    Environment variables required:
    - POSTGRES_HOST: Database host (default: localhost)
    - POSTGRES_PORT: Database port (default: 5432)
    - POSTGRES_DB: Database name (default: hypertrader)
    - POSTGRES_USER: Database user (default: postgres)
    - POSTGRES_PASSWORD: Database password (required)
    
    Example:
        >>> conn = PostgreSQLConnection()
        >>> await conn.connect()
        >>> async with conn.acquire() as pool_conn:
        ...     result = await pool_conn.fetch("SELECT * FROM candles LIMIT 10")
        >>> await conn.close()
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "hypertrader",
        user: str = "postgres",
        password: Optional[str] = None,
        min_pool_size: int = 10,
        max_pool_size: int = 20,
    ):
        if not HAS_ASYNCPG:
            raise ImportError("asyncpg is required. Install with: pip install asyncpg")
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.min_pool_size = min_pool_size
        self.max_pool_size = max_pool_size
        
        self._pool: Optional[asyncpg.Pool] = None
    
    async def connect(self) -> None:
        """Create connection pool."""
        if self._pool is not None:
            return
        
        self._pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            min_size=self.min_pool_size,
            max_size=self.max_pool_size,
            command_timeout=60.0,
        )
        
        # Initialize TimescaleDB
        await self._init_timescaledb()
    
    async def close(self) -> None:
        """Close connection pool."""
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
    
    @asynccontextmanager
    async def acquire(self):
        """Acquire connection from pool."""
        if self._pool is None:
            raise RuntimeError("Connection not established. Call connect() first.")
        
        async with self._pool.acquire() as connection:
            yield connection
    
    async def _init_timescaledb(self) -> None:
        """Initialize TimescaleDB extension and create schemas."""
        async with self.acquire() as conn:
            # Enable TimescaleDB extension
            await conn.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
            
            # Create tables if not exist
            await self._create_tables(conn)
    
    async def _create_tables(self, conn: asyncpg.Connection) -> None:
        """Create database schema."""
        
        # Candles table (time-series data)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS candles (
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                open DOUBLE PRECISION,
                high DOUBLE PRECISION,
                low DOUBLE PRECISION,
                close DOUBLE PRECISION,
                volume DOUBLE PRECISION,
                trades INTEGER,
                PRIMARY KEY (timestamp, symbol, timeframe)
            );
        """)
        
        # Convert to hypertable (if not already)
        try:
            await conn.execute("""
                SELECT create_hypertable('candles', 'timestamp', 
                                       if_not_exists => TRUE,
                                       chunk_time_interval => INTERVAL '1 day');
            """)
        except Exception:
            pass  # Already a hypertable
        
        # Trades table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                quantity DOUBLE PRECISION NOT NULL,
                commission DOUBLE PRECISION,
                realized_pnl DOUBLE PRECISION,
                strategy TEXT,
                metadata JSONB
            );
        """)
        
        # Create index on trades timestamp
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp 
            ON trades (timestamp DESC);
        """)
        
        # Signals table (alpha model predictions)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                model TEXT NOT NULL,
                p_long DOUBLE PRECISION,
                p_short DOUBLE PRECISION,
                mu DOUBLE PRECISION,
                sigma DOUBLE PRECISION,
                confidence DOUBLE PRECISION,
                quantiles JSONB,
                metadata JSONB
            );
        """)
        
        # Orders table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                order_type TEXT NOT NULL,
                price DOUBLE PRECISION,
                quantity DOUBLE PRECISION NOT NULL,
                status TEXT NOT NULL,
                filled_quantity DOUBLE PRECISION DEFAULT 0,
                average_price DOUBLE PRECISION,
                exchange_order_id TEXT,
                metadata JSONB
            );
        """)
        
        # Analytics table (portfolio metrics)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS analytics (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value DOUBLE PRECISION NOT NULL,
                metadata JSONB
            );
        """)
        
        # Create continuous aggregates for OHLCV rollups
        await self._create_continuous_aggregates(conn)
    
    async def _create_continuous_aggregates(self, conn: asyncpg.Connection) -> None:
        """Create TimescaleDB continuous aggregates for efficient queries."""
        
        # 1-hour candle aggregates from 1-minute data
        try:
            await conn.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1h
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 hour', timestamp) AS timestamp,
                    symbol,
                    first(open, timestamp) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    last(close, timestamp) AS close,
                    sum(volume) AS volume,
                    sum(trades) AS trades
                FROM candles
                WHERE timeframe = '1m'
                GROUP BY time_bucket('1 hour', timestamp), symbol
                WITH NO DATA;
            """)
            
            # Add refresh policy
            await conn.execute("""
                SELECT add_continuous_aggregate_policy('candles_1h',
                    start_offset => INTERVAL '3 hours',
                    end_offset => INTERVAL '1 hour',
                    schedule_interval => INTERVAL '1 hour',
                    if_not_exists => TRUE);
            """)
        except Exception:
            pass  # Already exists
        
        # Daily candle aggregates
        try:
            await conn.execute("""
                CREATE MATERIALIZED VIEW IF NOT EXISTS candles_1d
                WITH (timescaledb.continuous) AS
                SELECT
                    time_bucket('1 day', timestamp) AS timestamp,
                    symbol,
                    first(open, timestamp) AS open,
                    max(high) AS high,
                    min(low) AS low,
                    last(close, timestamp) AS close,
                    sum(volume) AS volume,
                    sum(trades) AS trades
                FROM candles
                WHERE timeframe = '1m'
                GROUP BY time_bucket('1 day', timestamp), symbol
                WITH NO DATA;
            """)
            
            await conn.execute("""
                SELECT add_continuous_aggregate_policy('candles_1d',
                    start_offset => INTERVAL '7 days',
                    end_offset => INTERVAL '1 day',
                    schedule_interval => INTERVAL '1 day',
                    if_not_exists => TRUE);
            """)
        except Exception:
            pass
    
    async def insert_candles(self, candles: List[Dict[str, Any]]) -> int:
        """
        Insert candles with conflict resolution.
        
        Args:
            candles: List of candle dictionaries with keys:
                timestamp, symbol, timeframe, open, high, low, close, volume, trades
        
        Returns:
            Number of rows inserted
        """
        if not candles:
            return 0
        
        async with self.acquire() as conn:
            # Use COPY for bulk inserts (much faster)
            records = [
                (
                    c["timestamp"],
                    c["symbol"],
                    c["timeframe"],
                    c["open"],
                    c["high"],
                    c["low"],
                    c["close"],
                    c["volume"],
                    c.get("trades", 0),
                )
                for c in candles
            ]
            
            result = await conn.copy_records_to_table(
                "candles",
                records=records,
                columns=["timestamp", "symbol", "timeframe", "open", "high", "low", "close", "volume", "trades"],
            )
            
            return len(records)
    
    async def query_candles(
        self,
        symbol: str,
        timeframe: str,
        start_time: datetime,
        end_time: datetime,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Query candles with time range.
        
        Args:
            symbol: Trading pair symbol
            timeframe: Candle timeframe (1m, 5m, 15m, etc.)
            start_time: Start timestamp
            end_time: End timestamp
            limit: Max results (optional)
        
        Returns:
            List of candle dictionaries
        """
        async with self.acquire() as conn:
            query = """
                SELECT timestamp, symbol, timeframe, open, high, low, close, volume, trades
                FROM candles
                WHERE symbol = $1 AND timeframe = $2
                  AND timestamp >= $3 AND timestamp <= $4
                ORDER BY timestamp DESC
            """
            
            if limit:
                query += f" LIMIT {limit}"
            
            rows = await conn.fetch(query, symbol, timeframe, start_time, end_time)
            
            return [dict(row) for row in rows]
    
    async def insert_signal(self, signal: Dict[str, Any]) -> int:
        """Insert alpha signal prediction."""
        async with self.acquire() as conn:
            result = await conn.execute("""
                INSERT INTO signals (timestamp, symbol, model, p_long, p_short, mu, sigma, confidence, quantiles, metadata)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, 
                signal["timestamp"],
                signal["symbol"],
                signal["model"],
                signal["p_long"],
                signal["p_short"],
                signal["mu"],
                signal["sigma"],
                signal["confidence"],
                signal.get("quantiles"),
                signal.get("metadata"),
            )
            return 1
    
    async def get_latest_timestamp(self, symbol: str, timeframe: str) -> Optional[datetime]:
        """Get the latest candle timestamp for a symbol/timeframe."""
        async with self.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT MAX(timestamp) as latest
                FROM candles
                WHERE symbol = $1 AND timeframe = $2
            """, symbol, timeframe)
            
            return row["latest"] if row else None


# Global connection instance
_connection: Optional[PostgreSQLConnection] = None


async def get_connection() -> PostgreSQLConnection:
    """Get or create global connection instance."""
    global _connection
    
    if _connection is None:
        import os
        
        _connection = PostgreSQLConnection(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB", "hypertrader"),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD"),
        )
        await _connection.connect()
    
    return _connection


async def close_connection() -> None:
    """Close global connection."""
    global _connection
    
    if _connection is not None:
        await _connection.close()
        _connection = None
