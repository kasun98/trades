import mlflow
import mlflow.pyfunc
import mlflow.xgboost
import xgboost as xgb
import time
import requests
import os
import pandas as pd
from logger.logger import setup_logger

logger = setup_logger("forecast-utils")

def create_xgb_model():
    model = xgb.XGBRegressor(
        objective="reg:squarederror",
        n_estimators=100,
        learning_rate=0.1,
        max_depth=6,
        random_state=42
    )
    logger.info("Created a new XGBoost model instance")
    return model

def load_best_model(experiment_name):
    model = None
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            raise ValueError(f"Experiment '{experiment_name}' not found")

        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["metrics.mse_t_5 ASC"],  # or accuracy, r2, etc.
        )
        
        best_run_id = runs.iloc[0]["run_id"]
        model_uri = f"runs:/{best_run_id}/model"
        model = mlflow.pyfunc.load_model(model_uri)
        
        logger.info(f"Loaded best model from experiment '{experiment_name}', run ID: {best_run_id}")
        return model

    except Exception as e:
        logger.error(f"Error loading best model from '{experiment_name}': {e}")
    
    return model

def rollback_to_version(target_run_id):
    model_uri = f"runs:/{target_run_id}/model"
    model = mlflow.pyfunc.load_model(model_uri)
    logger.info(f"Rolled back to model from Run ID: {target_run_id}")
    return model

def load_best_model_for_retrain(experiment_name):
    model = None
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment is None:
            raise ValueError(f"Experiment '{experiment_name}' not found")

        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["metrics.mse_t_5 ASC"],  # or accuracy, r2, etc.
        )
        
        best_run_id = runs.iloc[0]["run_id"]
        model_uri = f"runs:/{best_run_id}/model"
        model = mlflow.xgboost.load_model(model_uri)
        
        logger.info(f"Loaded best model from experiment '{experiment_name}', run ID: {best_run_id}")
        return model

    except Exception as e:
        logger.error(f"Error loading best model from '{experiment_name}': {e}")
    
    return model


# Example: Rollback to an older version (manually find a Run ID from MLflow UI)
# rollback_model = rollback_to_version("1234567890abcdef")


# Connect to the database and fetch data for 5min forcasting
# This function assumes that the database and tables have already been created

FEATURE_COLUMNS = [
                    "bid", "bid_qty", "ask", "ask_qty", "last",
                    "volume", "vwap", "low", "high", "change", "change_pct", "ma_5",
                    "ma_14", "ema_5", "std_14", "price_change", "price_change_pct",
                    "max_14", "min_14", "vwap_diff", "bid_ask_spread", "log_return",
                    "momentum", "volatility", "cumulative_volume", "volume_change",
                    "mean_bid_qty", "mean_ask_qty"
                ]

def connect_to_db(QDB_PORT=9000):
    try:
        tik = time.time()
        query = f"""
            SELECT 
                {', '.join(FEATURE_COLUMNS)}
            FROM gold_1min
            ORDER BY timestamp;
        """
        url = f"http://localhost:{QDB_PORT}/exec"
        res = requests.get(url, params={"query": query})
        json_data = res.json()

        # Extract column names
        column_names = [col["name"] for col in json_data["columns"]]
        rows = json_data["dataset"]

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=column_names)

        tok = time.time()
        logger.info(f"Time taken to fetch data: {tok - tik:.2f} seconds")

        if len(df) < 300:
            logger.info("Dataframe length may not be sufficient for training.")
            return None
        
        logger.info(f"Fetched data from QuestDB with {len(df)} rows and {len(df.columns)} columns")
        return df
    
    except Exception as e:
        logger.error(f"Error connecting to database : {e}")
        return None


