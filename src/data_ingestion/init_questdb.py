import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from ingestion_configs import (QDB_HOST, QDB_PSYCOPG_PORT, QDB_USER, QDB_PASSWORD, QDB_DB,
                               QDB_BRONZE_LAYER_TABLE, QDB_SILVER_LAYER_TABLE, QDB_EXPIRATION_DAYS_BRONZE, QDB_EXPIRATION_DAYS_SILVER,
                               QDB_GOLD_VIEW_1MIN, QDB_GOLD_VIEW_5MIN, QDB_GOLD_VIEW_EXPIRATION_HOURS,
                               QDB_EXPIRATION_DAYS_GOLD, QDB_GOLD_BID_PRED_TABLE, QDB_GOLD_VOL_PRED_TABLE,
                               QDB_BID_TASK_NAME, QDB_VOL_TASK_NAME)

from ingestion_utils import (init_questdb_client, create_bronze_layer_qdb, create_silver_layer_qdb, 
                             create_gold_view_qdb, create_gold_bid_pred_layer_qdb, create_gold_vol_pred_layer_qdb)

from logger.logger import setup_logger

logger = setup_logger("init_QDB")
logger.info("QuestDB initiating...")


def create_dwh_layers():
    # Create DWH layers
    try:
        # Initialize qdb connection
        conn = init_questdb_client(QDB_HOST, QDB_PSYCOPG_PORT, QDB_USER, QDB_PASSWORD)
        if not conn:
            logger.error("Failed to initialize connection.")
            raise Exception("QDB connection initialization failed.")
        
        # Bronze layer
        try:
            create_bronze_layer_qdb(conn, QDB_BRONZE_LAYER_TABLE, QDB_EXPIRATION_DAYS_BRONZE)
        except Exception as e:
            logger.error(f"Error creating bronze layer: {e}")
            raise

        # Silver layer
        try:
            create_silver_layer_qdb(conn, QDB_SILVER_LAYER_TABLE, QDB_EXPIRATION_DAYS_SILVER)
        except Exception as e:
            logger.error(f"Error creating silver layer: {e}")
            raise
        
        # Golden Views
        try:
            create_gold_view_qdb(conn, QDB_GOLD_VIEW_1MIN, QDB_SILVER_LAYER_TABLE, 1, QDB_GOLD_VIEW_EXPIRATION_HOURS)
            create_gold_view_qdb(conn, QDB_GOLD_VIEW_5MIN, QDB_SILVER_LAYER_TABLE, 5, QDB_GOLD_VIEW_EXPIRATION_HOURS)
            
        except Exception as e:
            logger.error(f"Error creating golden views: {e}")
            raise

        # Gold tables
        try:
            create_gold_bid_pred_layer_qdb(conn, QDB_GOLD_BID_PRED_TABLE, QDB_EXPIRATION_DAYS_GOLD)
            create_gold_vol_pred_layer_qdb(conn, QDB_GOLD_VOL_PRED_TABLE, QDB_EXPIRATION_DAYS_GOLD)
        except Exception as e:
            logger.error(f"Error creating gold tables: {e}")
            raise
        
        if conn:
            conn.close()
            logger.info("QDB connection closed.")

    except Exception as e:
        logger.error(f"Error connecting to QDB: {e}")
        raise



if __name__ == "__main__":
    try:
        create_dwh_layers()
        logger.info("QDB initialization completed.")
    except Exception as e:
        logger.error(f"Error in QDB initialization: {e}")
        raise
        