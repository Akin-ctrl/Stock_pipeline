"""
Train XGBoost models for stock direction prediction using the canonical dataset builder.
Logs models, parameters, and metrics to MLflow.
"""

import sys
import logging
from pathlib import Path
from datetime import date
import os

import pandas as pd
import xgboost as xgb
import mlflow
import mlflow.xgboost
from sklearn.metrics import roc_auc_score, accuracy_score, brier_score_loss

# Add app to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from app.config.database import get_db
from app.services.modeling.dataset_builder import (
    ModelingDatasetBuilder,
    ModelingDatasetConfig,
)
from app.services.modeling.targets import DirectionTargetDefinition
from app.services.modeling.feature_engineering import PROBABILITY_FEATURE_NAMES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
EXPERIMENT_NAME = "stock_direction_prediction"


def train_xgboost_model(
    db_session,
    horizon_days: int,
    start_date: date = None,
    end_date: date = None
):
    """
    Train and log an XGBoost model for a specific forward horizon.
    """
    logger.info(f"Building dataset for {horizon_days}-day horizon...")
    
    config = ModelingDatasetConfig(
        target_definition=DirectionTargetDefinition(horizon_trading_days=horizon_days),
        require_complete_data=True
    )
    
    builder = ModelingDatasetBuilder(db_session, config)
    rows = builder.build(start_date=start_date, end_date=end_date)
    
    if not rows:
        logger.error("No dataset rows generated.")
        return
        
    from app.services.modeling.feature_extractor import extract_probability_features_from_row
    
    processed_rows = []
    dropped_no_target = 0
    for row in rows:
        if row.target_up_10d is None:
            dropped_no_target += 1
            continue
        d = extract_probability_features_from_row(row)
        d['target_up_10d'] = row.target_up_10d
        d['anchor_date'] = row.anchor_date
        processed_rows.append(d)

    if dropped_no_target:
        drop_pct = dropped_no_target / len(rows) * 100
        logger.warning(
            f"Dropped {dropped_no_target}/{len(rows)} rows ({drop_pct:.1f}%) with "
            f"target_up_10d=None (horizon data not yet available)"
        )
        if drop_pct > 50:
            logger.error(
                f"Training dropout rate {drop_pct:.1f}% exceeds 50% — "
                f"model may be severely biased. Check price history coverage."
            )

    if not processed_rows:
        logger.error("No valid dataset rows with targets.")
        return
        
    df = pd.DataFrame(processed_rows)
    
    # Prepare features and target
    X = df[list(PROBABILITY_FEATURE_NAMES)]
    y = df['target_up_10d']
    
    # Time-based split (validation on latest 20%)
    # This prevents data leakage from the future
    df = df.sort_values('anchor_date')
    split_idx = int(len(df) * 0.8)
    
    X_train, y_train = X.iloc[:split_idx], y.iloc[:split_idx]
    X_test, y_test = X.iloc[split_idx:], y.iloc[split_idx:]
    
    logger.info(f"Training set: {len(X_train)} samples")
    logger.info(f"Test set: {len(X_test)} samples")
    
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)
    
    with mlflow.start_run(run_name=f"xgboost_{horizon_days}d"):
        mlflow.log_param("horizon_days", horizon_days)
        
        # XGBoost parameters
        params = {
            "objective": "binary:logistic",
            "eval_metric": "auc",
            "max_depth": 5,
            "learning_rate": 0.05,
            "n_estimators": 100,
            "subsample": 0.8,
            "colsample_bytree": 0.8,
            "random_state": 42
        }
        mlflow.log_params(params)
        
        model = xgb.XGBClassifier(**params)
        model.fit(
            X_train, y_train,
            eval_set=[(X_test, y_test)],
            verbose=False
        )
        
        # Evaluate
        y_pred_proba = model.predict_proba(X_test)[:, 1]
        y_pred = model.predict(X_test)
        
        auc = roc_auc_score(y_test, y_pred_proba)
        acc = accuracy_score(y_test, y_pred)
        brier = brier_score_loss(y_test, y_pred_proba)
        
        logger.info(f"Model Evaluation (Horizon: {horizon_days}d) - AUC: {auc:.4f}, Accuracy: {acc:.4f}, Brier: {brier:.4f}")
        
        mlflow.log_metric("test_auc", auc)
        mlflow.log_metric("test_accuracy", acc)
        mlflow.log_metric("test_brier_score", brier)
        
        # Log model
        from mlflow.models.signature import infer_signature
        signature = infer_signature(X_train, y_pred)
        
        mlflow.xgboost.log_model(
            xgb_model=model,
            artifact_path="model",
            signature=signature,
            registered_model_name=f"stock_direction_{horizon_days}d_model"
        )
        
        logger.info(f"Model saved to MLflow as 'stock_direction_{horizon_days}d_model'")


