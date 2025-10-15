"""
Database module for persistent storage.
Handles candle storage, signal tracking, and learning data.
"""

# Use SQLite for development (faster setup)
# Can upgrade to PostgreSQL/TimescaleDB later
from .connection_sqlite import DatabaseManager
from .candle_storage import CandleStorageService
from .signal_tracker import SignalTracker

__all__ = ['DatabaseManager', 'CandleStorageService', 'SignalTracker']
