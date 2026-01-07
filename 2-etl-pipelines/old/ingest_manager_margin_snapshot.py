"""
ETL Pipeline — Manager Margin Snapshot
--------------------------------------
Loads monthly margin snapshots per fund.  
Inserts into `manager_margin_snapshot`.
"""

import os
import json
import time
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine


# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


# ============================================================
# DB Connection
# ============================================================
def get_engine():
    return create_engine(
        "mysql+pymysql://user:password@localhost:3306/risk_db",
        pool_recycle=3600
    )


# ============================================================
# Load JSON (Simulated API)
# ============================================================
def fetch_fake_api(json_path: str) -> list:
    logging.info(f"Simulating API request: {json_path}")
    time.sleep(0.4)

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


# ============================================================
# Extract
# ============================================================
def extract(json_path: str) -> pd.DataFrame:
    logging.info("Extracting manager margin snapshot...")
    data = fetch_fake_api(json_path)
    df = pd.DataFrame(data)
    logging.info(f"Extracted {len(df)} rows.")
    return df


# ============================================================
# Transform
# ============================================================
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming manager margin snapshot...")

    rename_map = {
        "CgePortfolio": "fund_id",
        "DataEnvio": "sent_date",
        "MargemLocal": "local_margin",
        "MargemOffshore": "offshore_margin"
    }

    df = df.rename(columns=rename_map)

    df["sent_date"] = pd.to_datetime(df["sent_date"], errors="coerce")
    df["local_margin"] = pd.to_numeric(df["local_margin"], errors="coerce")
    df["offshore_margin"] = pd.to_numeric(df["offshore_margin"], errors="coerce")

    df["total_margin"] = df[["local_margin", "offshore_margin"]].abs().sum(axis=1)

    df["snapshot_timestamp"] = datetime.now()

    cols = [
        "fund_id",
        "sent_date",
        "local_margin",
        "offshore_margin",
        "total_margin",
        "snapshot_timestamp"
    ]

    df = df[cols]

    logging.info("Transformation complete.")
    return df


# ============================================================
# Load
# ============================================================
def load(df: pd.DataFrame, engine):
    logging.info("Loading into manager_margin_snapshot...")

    df.to_sql(
        "manager_margin_snapshot",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000
    )

    logging.info(f"Inserted {len(df)} rows into manager_margin_snapshot.")


# ============================================================
# Main
# ============================================================
def main():
    logging.info("Starting pipeline: MANAGER_MARGIN_SNAPSHOT")

    json_path = "../2-api-simulation/manager_margin_snapshot.json"
    engine = get_engine()

    try:
        raw = extract(json_path)
        clean = transform(raw)
        load(clean, engine)
        logging.info("Pipeline completed successfully.")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


# ============================================================
# Entry point
# ============================================================
if __name__ == "__main__":
    main()
