from flask import Flask, request, jsonify
import os
import numpy as np
import mlflow
import threading
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from forecast_configs import (MLFLOW_S3_ENDPOINT_URL, MLFLOW_TRACKING_URI, MINIO_USER_ID, MINIO_PASSWORD, AWS_DEFAULT_REGION,
                              MLFLOW_BID_PRICE_EXP, MLFLOW_VOLATILITY_EXP)
from utils import load_best_model # for prediction
from train_model import retrain # for retraining
import logging

# Initialize Flask app
app = Flask(__name__)

app.logger.setLevel(logging.INFO)

# Load the best model at the start of the app
MODEL_BID_PRICE = load_best_model(MLFLOW_BID_PRICE_EXP)
MODEL_VOLATILITY = load_best_model(MLFLOW_VOLATILITY_EXP)

# Set environment variables for MLflow and MinIO
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_USER_ID
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_PASSWORD
os.environ["AWS_DEFAULT_REGION"] = AWS_DEFAULT_REGION
os.environ["MLFLOW_TRACKING_URI"] = MLFLOW_TRACKING_URI

mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# API endpoint to test GET request
@app.route("/test", methods=["GET"])
def test():
    return jsonify({"status": "ok"}), 200

# API endpoint to test POST request
@app.route("/test_post", methods=["POST"])
def test_post():
    data = request.get_json()
    return jsonify({"received": data}), 200

# API endpoint to make predictions
@app.route("/predict_bid", methods=["POST"])
def predict_bid():
    try:
        data = request.get_json()
        features = data["data"]

        features_df = np.array(features).reshape(1, -1)
        
        prediction = MODEL_BID_PRICE.predict(features_df)

        return jsonify({"task": MLFLOW_BID_PRICE_EXP, "prediction": prediction.tolist()})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/predict_volatility", methods=["POST"])
def predict_volatility():
    try:
        data = request.get_json()
        features = data["data"]

        features_df = np.array(features).reshape(1, -1)
        
        prediction = MODEL_VOLATILITY.predict(features_df)

        return jsonify({"task": MLFLOW_VOLATILITY_EXP, "prediction": prediction.tolist()})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    

# Function to reload the model if necessary
@app.route("/reload", methods=["POST"])
def reload_model():
    global MODEL_BID_PRICE
    global MODEL_VOLATILITY

    MODEL_BID_PRICE = load_best_model(MLFLOW_BID_PRICE_EXP)
    MODEL_VOLATILITY = load_best_model(MLFLOW_VOLATILITY_EXP)

    return jsonify({"status": "Models reloaded successfully!"})

# API endpoint to retrain the model
@app.route("/retrain", methods=["POST"])
def retrain_model():
    def background_retrain():
        try:
            global MODEL_BID_PRICE
            global MODEL_VOLATILITY

            result_1 = retrain(MLFLOW_BID_PRICE_EXP)
            result_2 = retrain(MLFLOW_VOLATILITY_EXP)

            if result_1 == 1:
                MODEL_BID_PRICE = load_best_model(MLFLOW_BID_PRICE_EXP)
            else:
                print(f"Retrain {MLFLOW_BID_PRICE_EXP} failed.")

            if result_2 == 1:
                MODEL_VOLATILITY = load_best_model(MLFLOW_VOLATILITY_EXP)
            else:
                print(f"Retrain {MLFLOW_VOLATILITY_EXP} failed.")
        
        except Exception as e:
            print(f"Error during retraining: {e}")

    # Run in background thread to avoid blocking
    threading.Thread(target=background_retrain).start()

    return jsonify({"status": "Model retraining started."}), 200

# Run the app
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5050)
