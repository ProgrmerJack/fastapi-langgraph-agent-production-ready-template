"""
SQLite-based database manager for development.
Can be upgraded to PostgreSQL/TimescaleDB for production.

Note: On Docker with Windows volume mounts, SQLite cannot open files directly
on the mounted volume due to file locking incompatibility. We use a native
container path (/tmp/hypertrader/) for SQLite operations.
"""

import aiosqlite
import shutil
import sqlite3
from typing import Optional
import logging
from contextlib import asynccontextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

# Native container path for SQLite (avoids Windows volume mount issues)
NATIVE_DB_DIR = Path("/tmp/hypertrader")
NATIVE_DB_PATH = NATIVE_DB_DIR / "hypertrader.db"


def _get_sqlite_path(db_path: str = "data/hypertrader.db") -> Path:
    """
    Get the appropriate SQLite database path.
    
    On Docker with Windows volume mounts, SQLite files cannot be opened
    due to file locking incompatibility. We detect this and use a native
    container path instead.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Path to use for SQLite operations
    """
    mounted_path = Path(db_path)
    
    # Check if we're in Docker (presence of /.dockerenv or /run/.containerenv)
    in_docker = Path("/.dockerenv").exists() or Path("/run/.containerenv").exists()
    
    if not in_docker:
        # Not in Docker, use the normal path
        mounted_path.parent.mkdir(parents=True, exist_ok=True)
        return mounted_path
    
    # In Docker - Windows volume mounts don't support SQLite's WAL mode
    # due to file locking incompatibility. The issue manifests when trying
    # to use WAL mode on an EXISTING database file (new files work).
    # We test on the actual database file if it exists.
    try:
        mounted_path.parent.mkdir(parents=True, exist_ok=True)
        
        if mounted_path.exists():
            # Test WAL mode on the existing database - this is what fails on Windows volumes
            conn = sqlite3.connect(str(mounted_path), timeout=1.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.close()
        else:
            # Create test on a temporary file to check if volume supports WAL
            test_path = mounted_path.parent / "_sqlite_test.db"
            conn = sqlite3.connect(str(test_path), timeout=1.0)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE IF NOT EXISTS _test (id INTEGER)")
            conn.execute("INSERT OR REPLACE INTO _test VALUES (1)")
            conn.commit()
            conn.close()
            
            # Clean up test file and its WAL/SHM files
            for suffix in ["", "-wal", "-shm"]:
                try:
                    (test_path.parent / (test_path.name + suffix)).unlink()
                except FileNotFoundError:
                    pass
        
        # WAL mode worked - mounted path is safe to use
        logger.info(f"Using mounted SQLite path: {mounted_path}")
        return mounted_path
    except (sqlite3.OperationalError, OSError) as e:
        # Windows volume mount issue detected - use native path
        # WAL mode doesn't work over Windows -> Docker volume mounts
        logger.warning(f"Windows volume mount issue detected ({e}), using native path: {NATIVE_DB_PATH}")
        NATIVE_DB_DIR.mkdir(parents=True, exist_ok=True)
        
        # Copy existing DB from mounted path if it exists and native doesn't
        if mounted_path.exists() and not NATIVE_DB_PATH.exists():
            try:
                shutil.copy2(str(mounted_path), str(NATIVE_DB_PATH))
                logger.info(f"Copied database from {mounted_path} to {NATIVE_DB_PATH}")
            except Exception as e:
                logger.warning(f"Could not copy database: {e}")
        
        return NATIVE_DB_PATH

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
        # Get the appropriate path (handles Docker volume mount issues)
        resolved_path = _get_sqlite_path(database_path)
        self.database_path = str(resolved_path)
        self._connection: Optional[aiosqlite.Connection] = None
        
        # Ensure data directory exists
        resolved_path.parent.mkdir(parents=True, exist_ok=True)
    
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
