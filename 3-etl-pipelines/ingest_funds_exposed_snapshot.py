"""
ETL Pipeline — Funds Exposed Snapshot
-------------------------------------
This pipeline loads the daily “funds exposed” snapshot from a simulated API
and writes the cleaned dataset into the `risk_exposure_snapshot` table.

It follows a production-style ETL structure:
- fake API layer
- extract → transform → load
- structured logging
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
# Database Connection (placeholder)
# =============================================================================
def get_engine():
    """
    In a real scenario this would pull secrets from env vars or a secret store.
    For portfolio demonstration, credentials are kept simple.
    """
    return create_engine(
        "mysql+pymysql://user:password@localhost:3306/risk_db",
        pool_recycle=3600
    )


# =============================================================================
# Fake API Layer
# =============================================================================
def fetch_fake_api(json_path: str) -> dict:
    """
    Simulates an API call by:
    - adding slight artificial latency
    - validating the path
    - returning JSON payload

    This keeps the ETL realistic without exposing real endpoints.
    """
    logging.info("Simulating API request for funds_exposed_snapshot...")

    time.sleep(0.7)  # fake network latency

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Simulated API file not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# Extract
# =============================================================================
def extract(json_path: str) -> pd.DataFrame:
    logging.info(f"Extracting simulated payload: {json_path}")

    data = fetch_fake_api(json_path)
    df = pd.DataFrame(data)

    logging.info(f"Extracted {len(df)} records from funds_exposed snapshot.")
    return df


# =============================================================================
# Transform
# =============================================================================
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming funds_exposed dataset...")

    rename_map = {
        "CgePortfolio": "fund_id",
        "origem": "exposure_type",
        "dt_carga": "raw_timestamp"
    }

    df = df.rename(columns=rename_map)

    final_cols = [
        "fund_id",
        "exposure_type",
        "raw_timestamp"
    ]

    df = df[final_cols]

    df["raw_timestamp"] = pd.to_datetime(df["raw_timestamp"], errors="coerce")

    df["snapshot_timestamp"] = datetime.now()

    logging.info("Transformation complete.")
    return df


# =============================================================================
# Load
# =============================================================================
def load(df: pd.DataFrame, engine):
    logging.info("Loading dataset into risk_exposure_snapshot...")

    df.to_sql(
        "risk_exposure_snapshot",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    logging.info(f"Successfully inserted {len(df)} rows.")


# =============================================================================
# Main Pipeline
# =============================================================================
def main():
    logging.info("Starting pipeline: FUNDS_EXPOSED_SNAPSHOT")

    json_path = "../2-api-simulation/funds_exposed_snapshot.json"
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
