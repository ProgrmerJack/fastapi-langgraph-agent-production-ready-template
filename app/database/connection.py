"""
Database connection manager for PostgreSQL/TimescaleDB.
"""

import asyncpg
from typing import Optional
import logging
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class DatabaseManager:
    """
    Manages database connection pool for persistent storage.
    Supports PostgreSQL with TimescaleDB extension for time-series data.
    """
    
    def __init__(self, database_url: str, min_size: int = 5, max_size: int = 20):
        """
        Initialize database manager.
        
        Args:
            database_url: PostgreSQL connection string
            min_size: Minimum pool size
            max_size: Maximum pool size
        """
        self.database_url = database_url
        self.min_size = min_size
        self.max_size = max_size
        self._pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Establish database connection pool."""
        if self._pool is not None:
            logger.warning("Database pool already connected")
            return
        
        try:
            self._pool = await asyncpg.create_pool(
                self.database_url,
                min_size=self.min_size,
                max_size=self.max_size,
                command_timeout=60,
                timeout=30,
            )
            logger.info(f"Database pool created: {self.min_size}-{self.max_size} connections")
            
            # Verify TimescaleDB extension
            async with self._pool.acquire() as conn:
                version = await conn.fetchval("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'")
                if version:
                    logger.info(f"TimescaleDB extension detected: v{version}")
                else:
                    logger.warning("TimescaleDB extension not installed - performance may be suboptimal")
        
        except Exception as e:
            logger.error(f"Failed to create database pool: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection pool."""
        if self._pool is None:
            return
        
        await self._pool.close()
        self._pool = None
        logger.info("Database pool closed")
    
    @asynccontextmanager
    async def acquire(self):
        """
        Acquire a database connection from the pool.
        
        Usage:
            async with db.acquire() as conn:
                result = await conn.fetch("SELECT * FROM candles")
        """
        if self._pool is None:
            raise RuntimeError("Database pool not connected. Call connect() first.")
        
        async with self._pool.acquire() as connection:
            yield connection
    
    async def execute(self, query: str, *args):
        """Execute a query without returning results."""
        async with self.acquire() as conn:
            return await conn.execute(query, *args)
    
    async def fetch(self, query: str, *args):
        """Fetch multiple rows."""
        async with self.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchrow(self, query: str, *args):
        """Fetch a single row."""
        async with self.acquire() as conn:
            return await conn.fetchrow(query, *args)
    
    async def fetchval(self, query: str, *args):
        """Fetch a single value."""
        async with self.acquire() as conn:
            return await conn.fetchval(query, *args)
    
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
            timestamp TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            timeframe VARCHAR(10) NOT NULL,
            open DECIMAL(20, 8) NOT NULL,
            high DECIMAL(20, 8) NOT NULL,
            low DECIMAL(20, 8) NOT NULL,
            close DECIMAL(20, 8) NOT NULL,
            volume DECIMAL(20, 8) NOT NULL,
            quote_volume DECIMAL(20, 8),
            trades INTEGER,
            source VARCHAR(50) DEFAULT 'binance',
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (timestamp, symbol, timeframe)
        );
        
        -- Convert to hypertable for time-series optimization (TimescaleDB)
        -- This will fail gracefully if already a hypertable or if TimescaleDB not installed
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM timescaledb_information.hypertables 
                WHERE hypertable_name = 'candles'
            ) THEN
                PERFORM create_hypertable('candles', 'timestamp', if_not_exists => TRUE);
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'TimescaleDB not available or table already converted';
        END $$;
        
        -- Indexes for fast queries
        CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON candles (symbol, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_candles_timeframe ON candles (timeframe, timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_time ON candles (symbol, timeframe, timestamp DESC);
        
        -- Signal history table for learning
        CREATE TABLE IF NOT EXISTS signal_history (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            action VARCHAR(10) NOT NULL,  -- LONG, SHORT, HOLD
            confidence DECIMAL(5, 4),
            meta_score DECIMAL(10, 6),
            regime VARCHAR(50),
            
            -- Strategy component scores
            sentiment_macro DECIMAL(10, 6),
            sentiment_micro DECIMAL(10, 6),
            regime_score DECIMAL(10, 6),
            volatility_score DECIMAL(10, 6),
            trend_score DECIMAL(10, 6),
            
            -- Component weights at time of signal
            weight_sentiment DECIMAL(5, 4),
            weight_regime DECIMAL(5, 4),
            weight_volatility DECIMAL(5, 4),
            weight_trend DECIMAL(5, 4),
            
            -- Outcome tracking
            entry_price DECIMAL(20, 8),
            exit_price DECIMAL(20, 8),
            exit_timestamp TIMESTAMPTZ,
            pnl DECIMAL(20, 8),
            pnl_percent DECIMAL(10, 6),
            outcome VARCHAR(20),  -- WIN, LOSS, NEUTRAL, OPEN
            
            -- Learning metadata
            model_version VARCHAR(50),
            learning_iteration INTEGER,
            
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_signal_timestamp ON signal_history (timestamp DESC);
        CREATE INDEX IF NOT EXISTS idx_signal_outcome ON signal_history (outcome);
        CREATE INDEX IF NOT EXISTS idx_signal_symbol ON signal_history (symbol, timestamp DESC);
        
        -- Component performance tracking
        CREATE TABLE IF NOT EXISTS component_performance (
            id SERIAL PRIMARY KEY,
            component_name VARCHAR(100) NOT NULL,
            evaluation_timestamp TIMESTAMPTZ NOT NULL,
            
            -- Performance metrics
            total_signals INTEGER,
            winning_signals INTEGER,
            losing_signals INTEGER,
            win_rate DECIMAL(5, 4),
            avg_pnl DECIMAL(20, 8),
            sharpe_ratio DECIMAL(10, 6),
            
            -- Learned weights
            current_weight DECIMAL(5, 4),
            confidence_score DECIMAL(5, 4),
            influence_factor DECIMAL(5, 4),
            
            -- Decay factors
            recent_performance_7d DECIMAL(5, 4),
            recent_performance_30d DECIMAL(5, 4),
            
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_component_time ON component_performance (component_name, evaluation_timestamp DESC);
        
        -- Trade outcomes
        CREATE TABLE IF NOT EXISTS trade_outcomes (
            id SERIAL PRIMARY KEY,
            signal_id INTEGER REFERENCES signal_history(id),
            order_id VARCHAR(100),
            timestamp TIMESTAMPTZ NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            side VARCHAR(10),
            quantity DECIMAL(20, 8),
            price DECIMAL(20, 8),
            fee DECIMAL(20, 8),
            status VARCHAR(20),
            
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        
        CREATE INDEX IF NOT EXISTS idx_trade_signal ON trade_outcomes (signal_id);
        CREATE INDEX IF NOT EXISTS idx_trade_timestamp ON trade_outcomes (timestamp DESC);
        """
        
        try:
            async with self.acquire() as conn:
                await conn.execute(schema_sql)
            logger.info("Database schema initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize schema: {e}")
            raise
