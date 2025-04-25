#!/bin/bash

echo "Initializing QuestDB and MinIO..."
python src/data_ingestion/init_questdb.py
python src/data_ingestion/init_minio.py
echo "Databases initialized."