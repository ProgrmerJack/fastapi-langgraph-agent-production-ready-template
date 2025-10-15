"""
Alpha Models API Integration for Agent Backend.

Add these endpoints to serve alpha model predictions via FastAPI.
"""

from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

# These imports will work once the agent backend is updated
# from hypertrader.alphas import ALPHAS
# from hypertrader.alphas.data import create_features

router = APIRouter(prefix="/alphas", tags=["alphas"])


# ============================================================================
# Request/Response Models
# ============================================================================

class AlphaPredictionRequest(BaseModel):
    """Request for alpha prediction."""
    symbol: str
    timeframe: str = "1m"
    horizon: str = "15m"
    models: List[str] = ["xgboost"]  # List of models to run
    limit: int = 100  # Number of recent candles to use


class AlphaPredictionResponse(BaseModel):
    """Response with alpha predictions."""
    symbol: str
    timestamp: str
    horizon: str
    predictions: Dict[str, Dict]  # model_name -> prediction dict


class ModelInfo(BaseModel):
    """Information about an available model."""
    name: str
    type: str
    targets: List[str]
    horizon: str
    is_fitted: bool
    last_updated: Optional[str] = None


# ============================================================================
# API Endpoints
# ============================================================================

@router.get("/models", response_model=List[str])
async def list_models():
    """
    List all available alpha models.
    
    Returns:
        List of model names (e.g., ["xgboost", "lstm", "logit_linear"])
    """
    try:
        from hypertrader.alphas import ALPHAS
        return ALPHAS.list_models()
    except ImportError:
        return ["xgboost", "logit_linear", "lstm"]  # Default if not yet installed


@router.get("/models/{model_name}", response_model=ModelInfo)
async def get_model_info(model_name: str):
    """
    Get information about a specific model.
    
    Args:
        model_name: Name of the model (e.g., "xgboost")
        
    Returns:
        Model information including type, targets, and status
    """
    # This would load model metadata from disk
    model_path = Path(f"models/{model_name}")
    
    if not model_path.exists():
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found")
    
    # Load metadata
    import json
    metadata_path = model_path / "metadata.json"
    if not metadata_path.exists():
        raise HTTPException(status_code=500, detail="Model metadata not found")
    
    metadata = json.loads(metadata_path.read_text())
    
    return ModelInfo(
        name=metadata.get("name", model_name),
        type=model_name.split("_")[0],  # e.g., "xgboost" from "xgboost_15m"
        targets=metadata.get("targets", []),
        horizon=metadata.get("horizon", "15m"),
        is_fitted=True,
        last_updated=metadata.get("last_updated"),
    )


@router.post("/predict", response_model=AlphaPredictionResponse)
async def predict_alpha(request: AlphaPredictionRequest):
    """
    Generate alpha predictions for a symbol.
    
    This endpoint:
    1. Fetches recent OHLCV data for the symbol
    2. Engineers features
    3. Runs specified models
    4. Returns predictions
    
    Args:
        request: AlphaPredictionRequest with symbol, models, etc.
        
    Returns:
        Predictions from each requested model
    """
    try:
        # Import here to avoid dependency issues during startup
        from hypertrader.alphas import ALPHAS
        from hypertrader.alphas.data import create_features
        
        # 1. Fetch OHLCV data (integrate with existing market data API)
        # This would use the existing agent-backend market API
        # For now, we'll return a placeholder response
        
        # TODO: Integrate with actual market data
        # df = await fetch_market_data(request.symbol, request.timeframe, request.limit)
        
        # For demonstration, we'll show the expected flow:
        """
        # 2. Create features
        X = create_features(df)
        
        # 3. Run models
        predictions = {}
        for model_name in request.models:
            try:
                # Load model
                model_path = Path(f"models/{model_name}_{request.horizon}")
                model = ALPHAS.load(model_name, model_path)
                
                # Predict
                pred = model.predict_single(X)
                predictions[model_name] = pred.to_dict()
                
            except Exception as e:
                predictions[model_name] = {
                    "error": str(e)
                }
        
        # 4. Return response
        return AlphaPredictionResponse(
            symbol=request.symbol,
            timestamp=X.index[-1].isoformat(),
            horizon=request.horizon,
            predictions=predictions,
        )
        """
        
        # Placeholder response
        return AlphaPredictionResponse(
            symbol=request.symbol,
            timestamp=pd.Timestamp.now().isoformat(),
            horizon=request.horizon,
            predictions={
                model: {
                    "p_long": 0.55,
                    "p_short": 0.45,
                    "mu": 0.001,
                    "sigma": 0.005,
                    "confidence": 0.10,
                    "note": "Placeholder - integrate with actual models"
                }
                for model in request.models
            }
        )
        
    except ImportError:
        raise HTTPException(
            status_code=503,
            detail="Alpha models not installed. Run: pip install torch xgboost scikit-learn"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/train/{model_name}")
async def train_model(
    model_name: str,
    symbol: str,
    horizon: str = "15m",
    lookback_days: int = 30,
):
    """
    Train a model on historical data.
    
    This endpoint:
    1. Fetches historical data
    2. Creates features and targets
    3. Trains the model
    4. Saves to disk
    
    Args:
        model_name: Model to train (e.g., "xgboost")
        symbol: Trading symbol
        horizon: Forecast horizon
        lookback_days: Days of historical data
        
    Returns:
        Training results and metrics
    """
    # This would be a background task in production
    return {
        "status": "training_started",
        "model": model_name,
        "symbol": symbol,
        "horizon": horizon,
        "note": "Check /alphas/models/{model_name} for status"
    }


@router.get("/health")
async def health_check():
    """
    Check if alpha models system is healthy.
    
    Returns:
        System health status
    """
    try:
        from hypertrader.alphas import ALPHAS
        models = ALPHAS.list_models()
        return {
            "status": "healthy",
            "models_available": len(models),
            "models": models,
        }
    except ImportError:
        return {
            "status": "unhealthy",
            "error": "Alpha models not installed",
            "models_available": 0,
        }


# ============================================================================
# Integration Instructions
# ============================================================================

"""
To integrate with agent-backend:

1. Add to apps/agent-backend/app/main.py:
   
   from app.api.v1 import alphas
   app.include_router(alphas.router, prefix="/api/v1")

2. Create models directory:
   
   mkdir -p apps/agent-backend/models

3. Train models:
   
   python -m hypertrader.alphas.cli.train \\
       --model xgboost \\
       --data data/BTC-USDT.parquet \\
       --horizon 15m \\
       --output apps/agent-backend/models/xgboost_15m

4. Test endpoints:
   
   # List models
   curl http://localhost:8601/api/v1/alphas/models
   
   # Get predictions
   curl -X POST http://localhost:8601/api/v1/alphas/predict \\
       -H "Content-Type: application/json" \\
       -d '{
           "symbol": "BTC-USDT",
           "models": ["xgboost"],
           "horizon": "15m"
       }'

5. Dashboard integration:
   
   - Add "Alpha Signals" panel to dashboard
   - Poll /alphas/predict every 15 seconds
   - Display predictions with confidence
   - Show per-model signals on chart
"""
