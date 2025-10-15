"""
SQLite-based database manager for development.
Can be upgraded to PostgreSQL/TimescaleDB for production.
"""

import aiosqlite
from typing import Optional
import logging
from contextlib import asynccontextmanager
from pathlib import Path

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connection for persistent storage.
    Uses SQLite for simplicity - can migrate to PostgreSQL later.
    """
    
    def __init__(self, database_path: str = "data/hypertrader.db"):
        """
        Initialize database manager.
        
        Args:
            database_path: Path to SQLite database file
        """
        self.database_path = database_path
        self._connection: Optional[aiosqlite.Connection] = None
        
        # Ensure data directory exists
        Path(database_path).parent.mkdir(parents=True, exist_ok=True)
    
    async def connect(self):
        """Establish database connection."""
        if self._connection is not None:
            logger.warning("Database already connected")
            return
        
        try:
            self._connection = await aiosqlite.connect(
                self.database_path,
                timeout=30.0
            )
            # Enable WAL mode for better concurrency
            await self._connection.execute("PRAGMA journal_mode=WAL")
            await self._connection.execute("PRAGMA synchronous=NORMAL")
            await self._connection.commit()
            
            logger.info(f"Database connected: {self.database_path}")
        
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection."""
        if self._connection is None:
            return
        
        await self._connection.close()
        self._connection = None
        logger.info("Database closed")
    
    @asynccontextmanager
    async def acquire(self):
        """
        Acquire database connection.
        
        Usage:
            async with db.acquire() as conn:
                result = await conn.execute("SELECT * FROM candles")
        """
        if self._connection is None:
            raise RuntimeError("Database not connected. Call connect() first.")
        
        yield self._connection
    
    async def execute(self, query: str, *args):
        """Execute a query without returning results."""
        async with self.acquire() as conn:
            await conn.execute(query, args)
            await conn.commit()
    
    async def fetch(self, query: str, *args):
        """Fetch multiple rows."""
        async with self.acquire() as conn:
            cursor = await conn.execute(query, args)
            rows = await cursor.fetchall()
            # Get column names
            columns = [description[0] for description in cursor.description] if cursor.description else []
            # Convert to dictionaries
            return [dict(zip(columns, row)) for row in rows]
    
    async def fetchrow(self, query: str, *args):
        """Fetch a single row."""
        async with self.acquire() as conn:
            cursor = await conn.execute(query, args)
            row = await cursor.fetchone()
            if not row:
                return None
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
    
    async def fetchval(self, query: str, *args):
        """Fetch a single value."""
        async with self.acquire() as conn:
            cursor = await conn.execute(query, args)
            row = await cursor.fetchone()
            return row[0] if row else None
    
    async def health_check(self) -> bool:
        """Check if database is healthy."""
        try:
            result = await self.fetchval("SELECT 1")
            return result == 1
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False
    
    async def initialize_schema(self):
        """
        Create database schema for candle storage and learning.
        Idempotent - safe to call multiple times.
        """
        schema_sql = """
        -- Candle storage table
        CREATE TABLE IF NOT EXISTS candles (
            timestamp INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            quote_volume REAL,
            trades INTEGER,
            source TEXT DEFAULT 'binance',
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            PRIMARY KEY (timestamp, symbol, timeframe)
        );
        
        CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON candles (symbol, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_candles_timeframe ON candles (timeframe, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_time ON candles (symbol, timeframe, timestamp DESC);
        
        -- Signal history table for learning
        CREATE TABLE IF NOT EXISTS signal_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            action TEXT NOT NULL,
            confidence REAL,
            meta_score REAL,
            regime TEXT,
            
            -- Strategy component scores
            sentiment_macro REAL,
            sentiment_micro REAL,
            regime_score REAL,
            volatility_score REAL,
            trend_score REAL,
            
            -- Component weights at time of signal
            weight_sentiment REAL,
            weight_regime REAL,
            weight_volatility REAL,
            weight_trend REAL,
            
            -- Outcome tracking
            entry_price REAL,
            exit_price REAL,
            exit_timestamp INTEGER,
            pnl REAL,
            pnl_percent REAL,
            outcome TEXT DEFAULT 'OPEN',
            
            -- Learning metadata
            model_version TEXT,
            learning_iteration INTEGER,
            
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        );
        
        CREATE INDEX IF NOT EXISTS idx_signal_timestamp ON signal_history (timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_signal_outcome ON signal_history (outcome);
        CREATE INDEX IF NOT EXISTS idx_signal_symbol ON signal_history (symbol, timestamp DESC);
        
        -- Component performance tracking
        CREATE TABLE IF NOT EXISTS component_performance (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            component_name TEXT NOT NULL,
            evaluation_timestamp INTEGER NOT NULL,
            
            -- Performance metrics
            total_signals INTEGER,
            winning_signals INTEGER,
            losing_signals INTEGER,
            win_rate REAL,
            avg_pnl REAL,
            sharpe_ratio REAL,
            
            -- Learned weights
            current_weight REAL,
            confidence_score REAL,
            influence_factor REAL,
            
            -- Decay factors
            recent_performance_7d REAL,
            recent_performance_30d REAL,
            
            created_at INTEGER DEFAULT (strftime('%s', 'now'))
        );
        
        CREATE INDEX IF NOT EXISTS idx_component_time ON component_performance (component_name, evaluation_timestamp DESC);
        
        -- Trade outcomes
        CREATE TABLE IF NOT EXISTS trade_outcomes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id INTEGER,
            order_id TEXT,
            timestamp INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT,
            quantity REAL,
            price REAL,
            fee REAL,
            status TEXT,
            
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            FOREIGN KEY (signal_id) REFERENCES signal_history(id)
        );
        
        CREATE INDEX IF NOT EXISTS idx_trade_signal ON trade_outcomes (signal_id);
        CREATE INDEX IF NOT EXISTS idx_trade_timestamp ON trade_outcomes (timestamp DESC);
        """
        
        try:
            async with self.acquire() as conn:
                await conn.executescript(schema_sql)
                await conn.commit()
            logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise
