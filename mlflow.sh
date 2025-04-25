#!/bin/bash

echo "Initializing MLflow server..."

export MLFLOW_S3_ENDPOINT_URL="http://localhost:9002"
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_DEFAULT_REGION="us-east-1"

mlflow server \
  --backend-store-uri "postgresql://mlflow_user:mlflow_pass@localhost:5431/mlflow_db" \
  --artifacts-destination "s3://mlflow/" \
  --host 0.0.0.0 \
  --port 5000
