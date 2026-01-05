"""
ETL Pipeline — Funds AUM History
---------------------------------
Loads historical AUM (monthly or periodic snapshots) from a simulated API
and inserts it into the `funds_aum_history` table.

Follows the same robust structure used across the project.
"""

import os
import json
import time
import logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine


# =============================================================================
# Logging Setup
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# =============================================================================
# DB Connection
# =============================================================================
def get_engine():
    return create_engine(
        "mysql+pymysql://user:password@localhost:3306/risk_db",
        pool_recycle=3600
    )


# =============================================================================
# Fake API Fetch
# =============================================================================
def fetch_fake_api(json_path: str) -> dict:
    logging.info("Simulating API request for AUM history...")
    time.sleep(0.6)

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"File not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# Extract
# =============================================================================
def extract(json_path: str) -> pd.DataFrame:
    logging.info(f"Extracting AUM history snapshot: {json_path}")
    data = fetch_fake_api(json_path)
    df = pd.DataFrame(data)
    logging.info(f"Extracted {len(df)} rows from AUM history JSON.")
    return df


# =============================================================================
# Transform
# =============================================================================
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming AUM history dataset...")

    rename_map = {
        "cgePortfolio": "fund_id",
        "data": "as_of_date",
        "PatrimonioAbertura": "aum_open",
        "PatrimonioFechamento": "aum_close",
        "dt_carga": "raw_timestamp"
    }

    df = df.rename(columns=rename_map)

    final_cols = [
        "fund_id",
        "as_of_date",
        "aum_open",
        "aum_close",
        "raw_timestamp"
    ]

    df = df[final_cols]

    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df["raw_timestamp"] = pd.to_datetime(df["raw_timestamp"], errors="coerce")

    df["snapshot_timestamp"] = datetime.now()

    logging.info("AUM history transformation complete.")
    return df


# =============================================================================
# Load
# =============================================================================
def load(df: pd.DataFrame, engine):
    logging.info("Loading dataset into funds_aum_history...")

    df.to_sql(
        "funds_aum_history",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    logging.info(f"Inserted {len(df)} rows into funds_aum_history.")


# =============================================================================
# Main
# =============================================================================
def main():
    logging.info("Starting pipeline: FUNDS_AUM_HISTORY")

    json_path = "../2-api-simulation/funds_aum_history.json"
    engine = get_engine()

    try:
        raw = extract(json_path)
        clean = transform(raw)
        load(clean, engine)
        logging.info("Pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


# =============================================================================
# Entry Point
# =============================================================================
if __name__ == "__main__":
    main()
