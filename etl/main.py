import json
import snowflake.connector
import logging
import os
import time

# Ensure logs directory exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/etl.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

from load import (
    create_tables,
    load_tfl_status,
    load_line_disruptions,
    load_arrivals,
    load_journeys,
    load_station_status
)
from extract import (
    fetch_line_status,
    fetch_disruptions,
    fetch_arrivals,
    fetch_station_status,
    fetch_journey
)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "..", "config", "config.json")
CONFIG_PATH = os.path.abspath(CONFIG_PATH)

POLL_INTERVAL = 60  # seconds

def etl_run(cur, conn):
    # Step 3: Create Tables
    create_tables(cur)
    logger.info("✅ Tables ready")

    # Step 4: Extract + Load
    status_data = fetch_line_status()
    load_tfl_status(cur, conn, status_data)
    logger.info("✅ Loaded Line Status")

    disruptions_data = fetch_disruptions()
    load_line_disruptions(cur, conn, disruptions_data)
    logger.info("✅ Loaded Disruptions")

    arrivals_data = fetch_arrivals()
    load_arrivals(cur, conn, arrivals_data)
    logger.info("✅ Loaded Arrivals")

    station_data = fetch_station_status()
    load_station_status(cur, conn, station_data)
    logger.info("✅ Loaded Station Status")

    journey_data = fetch_journey()
    load_journeys(cur, conn, journey_data)
    logger.info("✅ Loaded Journeys")

if __name__ == "__main__":
    with open(CONFIG_PATH, "r") as f:
        config = json.load(f)
    SNOWFLAKE_CFG = config["snowflake"]

    while True:
        try:
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_CFG["user"],
                password=SNOWFLAKE_CFG["password"],
                account=SNOWFLAKE_CFG["account"],
                warehouse=SNOWFLAKE_CFG["warehouse"],
                database=SNOWFLAKE_CFG["database"],
                schema=SNOWFLAKE_CFG["schema"]
            )
            cur = conn.cursor()
            logger.info("✅ Connected to Snowflake")

            etl_run(cur, conn)

            cur.close()
            conn.close()
            logger.info("✅ ETL Pipeline Complete! Waiting for next poll...")

        except Exception as e:
            logger.error(f"ETL run failed: {e}")

        time.sleep(POLL_INTERVAL)