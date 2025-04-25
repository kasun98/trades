from minio import Minio
from minio.error import S3Error
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from ingestion_configs import MINIO_HOST, MINIO_USER_ID, MINIO_PASSWORD, MINIO_MLFLOW_BUCKET, MINIO_DATALAKE_BUCKET
from logger.logger import setup_logger

logger = setup_logger("init_MINIO")
logger.info("MINIO initiating...")

def main():
    
    try:
        minio_client = Minio(
                        MINIO_HOST,
                        access_key=MINIO_USER_ID,
                        secret_key=MINIO_PASSWORD,
                        secure=False  # Set to True if using HTTPS
                    )
    except S3Error as err:
        logger.error(f"Error connecting to MinIO: {err}")
        return

    # Buckets to create
    buckets = [MINIO_MLFLOW_BUCKET, MINIO_DATALAKE_BUCKET]

    if minio_client:
        logger.info("Connected to MinIO successfully.")
        for bucket in buckets:
            try:
                if not minio_client.bucket_exists(bucket):
                    minio_client.make_bucket(bucket)
                    logger.info(f"Bucket '{bucket}' created.")
                else:
                    logger.info(f"Bucket '{bucket}' already exists.")
            except S3Error as err:
                logger.error(f"Error creating bucket '{bucket}': {err}")
    
    else:
        logger.error("Failed to connect to MinIO.")


if __name__ == "__main__":
    main()
