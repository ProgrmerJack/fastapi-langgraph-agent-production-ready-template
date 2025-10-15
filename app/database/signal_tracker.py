"""
Signal tracking service for machine learning feedback loop.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime
import logging

# Import will be from connection_sqlite via __init__
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from .connection_sqlite import DatabaseManager
else:
    DatabaseManager = Any  # type: ignore

logger = logging.getLogger(__name__)


class SignalTracker:
    """
    Tracks trading signals and their outcomes for learning.
    Enables feedback loop: signal → action → outcome → weight update.
    """
    
    def __init__(self, db: DatabaseManager):
        """
        Initialize signal tracker.
        
        Args:
            db: Database manager instance
        """
        self.db = db
    
    async def record_signal(
        self,
        timestamp: datetime,
        symbol: str,
        action: str,
        confidence: float,
        meta_score: float,
        regime: str,
        component_scores: Dict[str, float],
        component_weights: Dict[str, float],
        entry_price: float,
        model_version: str = 'v1',
        learning_iteration: int = 0
    ) -> int:
        """
        Record a trading signal when generated.
        
        Args:
            timestamp: Signal timestamp
            symbol: Trading pair
            action: LONG, SHORT, or HOLD
            confidence: Signal confidence (0-1)
            meta_score: Weighted meta score
            regime: Market regime
            component_scores: Scores for each strategy component
            component_weights: Weights for each component
            entry_price: Entry price if trade taken
            model_version: ML model version
            learning_iteration: Learning iteration number
        
        Returns:
            Signal ID for tracking outcomes
        """
        try:
            ts_unix = int(timestamp.timestamp()) if isinstance(timestamp, datetime) else timestamp
            
            # SQLite uses lastrowid for getting inserted ID
            async with self.db.acquire() as conn:
                cursor = await conn.execute("""
                    INSERT INTO signal_history (
                        timestamp, symbol, action, confidence, meta_score, regime,
                        sentiment_macro, sentiment_micro, regime_score, volatility_score, trend_score,
                        weight_sentiment, weight_regime, weight_volatility, weight_trend,
                        entry_price, model_version, learning_iteration, outcome
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?, ?, 'OPEN'
                    )
                """, (
                    ts_unix, symbol, action, confidence, meta_score, regime,
                    component_scores.get('sentiment_macro', 0),
                    component_scores.get('sentiment_micro', 0),
                    component_scores.get('regime', 0),
                    component_scores.get('volatility', 0),
                    component_scores.get('trend', 0),
                    component_weights.get('sentiment', 0),
                    component_weights.get('regime', 0),
                    component_weights.get('volatility', 0),
                    component_weights.get('trend', 0),
                    entry_price,
                    model_version, learning_iteration
                ))
                await conn.commit()
                signal_id = cursor.lastrowid
            
            logger.info(f"Recorded signal {signal_id} for {symbol}: {action} @ {entry_price}")
            return signal_id
        
        except Exception as e:
            logger.error(f"Failed to record signal: {e}")
            raise
    
    async def record_outcome(
        self,
        signal_id: int,
        exit_price: float,
        exit_timestamp: datetime,
        pnl: float,
        outcome: str
    ) -> None:
        """
        Update signal with outcome when position closes.
        
        Args:
            signal_id: Signal ID from record_signal
            exit_price: Exit price
            exit_timestamp: Exit timestamp
            pnl: Profit/loss
            outcome: WIN, LOSS, or NEUTRAL
        """
        try:
            ts_unix = int(exit_timestamp.timestamp()) if isinstance(exit_timestamp, datetime) else exit_timestamp
            
            # Get entry price to calculate P&L percent
            entry_price = await self.db.fetchval(
                "SELECT entry_price FROM signal_history WHERE id = ?",
                signal_id
            )
            
            if entry_price:
                pnl_percent = (exit_price - entry_price) / entry_price * 100
            else:
                pnl_percent = 0
            
            await self.db.execute("""
                UPDATE signal_history
                SET exit_price = ?,
                    exit_timestamp = ?,
                    pnl = ?,
                    pnl_percent = ?,
                    outcome = ?
                WHERE id = ?
            """, exit_price, ts_unix, pnl, pnl_percent, outcome, signal_id)
            
            logger.info(f"Recorded outcome for signal {signal_id}: {outcome} (P&L: {pnl:.2f})")
        
        except Exception as e:
            logger.error(f"Failed to record outcome: {e}")
            raise
    
    async def get_signal(self, signal_id: int) -> Optional[Dict[str, Any]]:
        """Get signal details by ID."""
        try:
            row = await self.db.fetchrow("""
                SELECT * FROM signal_history WHERE id = $1
            """, signal_id)
            
            if not row:
                return None
            
            return dict(row)
        
        except Exception as e:
            logger.error(f"Failed to get signal: {e}")
            return None
    
    async def get_recent_signals(
        self,
        symbol: Optional[str] = None,
        limit: int = 100,
        outcome: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        Get recent signals with optional filtering.
        
        Args:
            symbol: Filter by symbol (optional)
            limit: Maximum number of signals
            outcome: Filter by outcome (WIN, LOSS, NEUTRAL, OPEN)
        
        Returns:
            List of signal dictionaries
        """
        query = "SELECT * FROM signal_history WHERE 1=1"
        params = []
        param_count = 0
        
        if symbol:
            param_count += 1
            query += f" AND symbol = ${param_count}"
            params.append(symbol)
        
        if outcome:
            param_count += 1
            query += f" AND outcome = ${param_count}"
            params.append(outcome)
        
        query += " ORDER BY timestamp DESC"
        
        param_count += 1
        query += f" LIMIT ${param_count}"
        params.append(limit)
        
        try:
            rows = await self.db.fetch(query, *params)
            return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Failed to get recent signals: {e}")
            return []
    
    async def calculate_component_performance(
        self,
        component_name: str,
        lookback_days: int = 30
    ) -> Dict[str, Any]:
        """
        Calculate performance metrics for a strategy component.
        
        Args:
            component_name: Name of component (sentiment, regime, volatility, trend)
            lookback_days: Days of history to analyze
        
        Returns:
            Performance metrics dictionary
        """
        try:
            # Map component name to database columns
            score_col = f"{component_name}_score" if component_name != 'sentiment' else 'sentiment_macro'
            weight_col = f"weight_{component_name}"
            
            stats = await self.db.fetchrow(f"""
                SELECT 
                    COUNT(*) as total_signals,
                    COUNT(*) FILTER (WHERE outcome = 'WIN') as winning_signals,
                    COUNT(*) FILTER (WHERE outcome = 'LOSS') as losing_signals,
                    AVG(CASE WHEN outcome = 'WIN' THEN 1.0 ELSE 0.0 END) as win_rate,
                    AVG(pnl) as avg_pnl,
                    STDDEV(pnl) as stddev_pnl,
                    AVG({weight_col}) as avg_weight,
                    AVG({score_col}) as avg_score
                FROM signal_history
                WHERE timestamp >= NOW() - INTERVAL '{lookback_days} days'
                  AND outcome IN ('WIN', 'LOSS')
            """)
            
            if not stats or stats['total_signals'] == 0:
                return {
                    'component_name': component_name,
                    'total_signals': 0,
                    'win_rate': 0,
                    'avg_pnl': 0,
                    'sharpe_ratio': 0,
                    'avg_weight': 0,
                    'avg_score': 0
                }
            
            # Calculate Sharpe ratio (simplified)
            sharpe = (float(stats['avg_pnl']) / float(stats['stddev_pnl'])) if stats['stddev_pnl'] and stats['stddev_pnl'] > 0 else 0
            
            return {
                'component_name': component_name,
                'total_signals': stats['total_signals'],
                'winning_signals': stats['winning_signals'],
                'losing_signals': stats['losing_signals'],
                'win_rate': float(stats['win_rate']) if stats['win_rate'] else 0,
                'avg_pnl': float(stats['avg_pnl']) if stats['avg_pnl'] else 0,
                'sharpe_ratio': sharpe,
                'avg_weight': float(stats['avg_weight']) if stats['avg_weight'] else 0,
                'avg_score': float(stats['avg_score']) if stats['avg_score'] else 0
            }
        
        except Exception as e:
            logger.error(f"Failed to calculate component performance: {e}")
            return {}
    
    async def save_component_performance(
        self,
        component_name: str,
        metrics: Dict[str, Any],
        current_weight: float,
        confidence_score: float
    ) -> None:
        """Save component performance metrics to database."""
        try:
            import time
            ts_unix = int(time.time())
            
            await self.db.execute("""
                INSERT INTO component_performance (
                    component_name, evaluation_timestamp,
                    total_signals, winning_signals, losing_signals,
                    win_rate, avg_pnl, sharpe_ratio,
                    current_weight, confidence_score, influence_factor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                component_name, ts_unix,
                metrics.get('total_signals', 0),
                metrics.get('winning_signals', 0),
                metrics.get('losing_signals', 0),
                metrics.get('win_rate', 0),
                metrics.get('avg_pnl', 0),
                metrics.get('sharpe_ratio', 0),
                current_weight,
                confidence_score,
                current_weight * confidence_score
            )
            
            logger.info(f"Saved performance for {component_name}: WR={metrics.get('win_rate', 0):.2%}, Weight={current_weight:.3f}")
        
        except Exception as e:
            logger.error(f"Failed to save component performance: {e}")
    
    async def get_learning_data(
        self,
        lookback_days: int = 30,
        min_signals: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get training data for machine learning.
        Returns signals with outcomes for learning.
        
        Args:
            lookback_days: Days of history to include
            min_signals: Minimum signals required
        
        Returns:
            List of signal dictionaries with outcomes
        """
        try:
            rows = await self.db.fetch("""
                SELECT *
                FROM signal_history
                WHERE timestamp >= NOW() - INTERVAL '$1 days'
                  AND outcome IN ('WIN', 'LOSS')
                ORDER BY timestamp DESC
            """, lookback_days)
            
            signals = [dict(row) for row in rows]
            
            if len(signals) < min_signals:
                logger.warning(f"Only {len(signals)} signals available, need {min_signals} for learning")
            
            return signals
        
        except Exception as e:
            logger.error(f"Failed to get learning data: {e}")
            return []
    
    async def get_statistics(self) -> Dict[str, Any]:
        """Get overall signal statistics."""
        try:
            stats = await self.db.fetchrow("""
                SELECT 
                    COUNT(*) as total_signals,
                    SUM(CASE WHEN outcome = 'WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN outcome = 'LOSS' THEN 1 ELSE 0 END) as losses,
                    SUM(CASE WHEN outcome = 'OPEN' THEN 1 ELSE 0 END) as open_positions,
                    AVG(CASE WHEN outcome IN ('WIN', 'LOSS') THEN pnl END) as avg_pnl,
                    SUM(CASE WHEN outcome IN ('WIN', 'LOSS') THEN pnl ELSE 0 END) as total_pnl,
                    MIN(timestamp) as first_signal,
                    MAX(timestamp) as last_signal
                FROM signal_history
            """)
            
            if not stats:
                return {}
            
            win_rate = 0
            if stats['wins'] and (stats['wins'] + stats['losses']) > 0:
                win_rate = stats['wins'] / (stats['wins'] + stats['losses'])
            
            return {
                'total_signals': stats['total_signals'],
                'wins': stats['wins'],
                'losses': stats['losses'],
                'open_positions': stats['open_positions'],
                'win_rate': win_rate,
                'avg_pnl': float(stats['avg_pnl']) if stats['avg_pnl'] else 0,
                'total_pnl': float(stats['total_pnl']) if stats['total_pnl'] else 0,
                'first_signal': datetime.fromtimestamp(stats['first_signal']) if stats['first_signal'] else None,
                'last_signal': datetime.fromtimestamp(stats['last_signal']) if stats['last_signal'] else None
            }
        
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {}
