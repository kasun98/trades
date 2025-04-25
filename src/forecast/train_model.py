
from utils import load_best_model_for_retrain, connect_to_db, FEATURE_COLUMNS
import xgboost as xgb
import os
import mlflow.xgboost
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import requests
from forecast_configs import MLFLOW_BID_PRICE_EXP, MLFLOW_VOLATILITY_EXP, MLFLOW_TRACKING_URI
from logger.logger import setup_logger

logger = setup_logger("train-models")


forecast_horizon = 5
feature_cols = FEATURE_COLUMNS

def model_retrain(qdb_query, model, task, forecast_horizon=5):
    
    # Create future target columns
    for i in range(1, forecast_horizon + 1):
        qdb_query[f"{task}_t+{i}"] = qdb_query[f"{task}"].shift(-i)

    # Drop rows with NaN (caused by shifting forward)
    qdb_query.dropna(inplace=True)

    # Define target columns
    target_cols = [f"{task}_t+{i}" for i in range(1, forecast_horizon + 1)]

    # Split data (80% train, 20% test)
    train_size = int(len(qdb_query) * 0.8)
    train, test = qdb_query.iloc[:train_size], qdb_query.iloc[train_size:]

    X_train, y_train = train[feature_cols], train[target_cols]
    X_test, y_test = test[feature_cols], test[target_cols]

    model.fit(X_train, y_train)

    # Predict on test set
    y_pred = model.predict(X_test)

    # Evaluate the mean absolute error for each forecast step
    mae = mean_absolute_error(y_test, y_pred, multioutput="raw_values")
    mse = mean_squared_error(y_test, y_pred, multioutput="raw_values")
    r2 = r2_score(y_test, y_pred, multioutput="raw_values")

    # Log model to MLflow
    with mlflow.start_run():
        mlflow.log_param("model_type", "XGBoost")

        for i in range(forecast_horizon):
            mlflow.log_metric(f"mae_t_{i+1}", mae[i])
            mlflow.log_metric(f"mse_t_{i+1}", mse[i])
            mlflow.log_metric(f"r2_t_{i+1}", r2[i])

        # Log the model
        mlflow.xgboost.log_model(model, "model")


def retrain(experiment_name):
    try:
        
        mlflow.set_experiment(experiment_name)
        # mlflow.xgboost.autolog(log_model=True, disable=True)
        # Load the best model from MLflow
        best_model = load_best_model_for_retrain(experiment_name)
        logger.info("Loaded the best model from model registry")
    except Exception as e:
        logger.error(f"Error loading model: {e}")
        return 0

    try:
        # Connect to the database and fetch data
        df = connect_to_db()
        logger.info("Connected to DB and fetch the data")

    except Exception as e:
        logger.error(f"Error connecting to database and fetch data as pandas dataframe: {e}")
        return 0

    if df is not None:
        if experiment_name == MLFLOW_BID_PRICE_EXP:
            try:
                model_retrain(df, best_model, "bid")
                logger.info(f"XGBoost model retrained for {MLFLOW_BID_PRICE_EXP}.") 
                return 1
                
            except Exception as e:
                logger.error(f"{MLFLOW_BID_PRICE_EXP} Model retraining aborted due to {e}")
                return 0
        
        elif experiment_name == MLFLOW_VOLATILITY_EXP:
            try:
                model_retrain(df, best_model, "volatility")
                logger.info(f"XGBoost model retrained for {MLFLOW_VOLATILITY_EXP}.")
                return 1
                
            except Exception as e:
                logger.error(f"{MLFLOW_VOLATILITY_EXP} Model retraining aborted due to {e}")
                return 0

    else:
        logger.error("Model or data is None or insufficient for training. Cannot proceed with training.")
        return 0
