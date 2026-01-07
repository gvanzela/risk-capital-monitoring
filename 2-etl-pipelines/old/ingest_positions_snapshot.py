"""
ETL Pipeline — Positions Snapshot
---------------------------------
Loads position-level snapshots for each exposed fund, based on the last
valid AUM date. Inserts records into `funds_positions_snapshot`.

This follows the same structured, professional ETL format used across
the repository.
"""

import os
import json
import time
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine


# =============================================================================
# Logging
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
# Load JSON (Simulated API)
# =============================================================================
def fetch_fake_api(json_path: str) -> list:
    logging.info(f"Simulating API request: {json_path}")
    time.sleep(0.6)

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)


# =============================================================================
# Extract
# =============================================================================
def extract(json_path: str) -> pd.DataFrame:
    logging.info("Extracting positions snapshot...")

    data = fetch_fake_api(json_path)
    df = pd.DataFrame(data)

    logging.info(f"Extracted {len(df)} raw rows from positions JSON.")
    return df


# =============================================================================
# Transform
# =============================================================================
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming positions snapshot...")

    rename_map = {
        "Nickname": "asset_symbol",
        "DataCarteira": "as_of_date",
        "notional": "notional_amount",
        "CgePortfolio": "fund_id",
        "ValorCotacao": "mark_price",
        "NmClassificacao": "classification",
        "qtyposicao": "quantity",
        "IdClassificacao": "classification_id",
        "valorfinanceiro": "financial_value",
        "CodAtivo": "asset_id",
        "NuIsin": "isin",
        "CodTipoAtivo": "asset_type_id",
        "dt_carteira": "aquisition_date"
    }

    df = df.rename(columns=rename_map)

    cols = [
        "fund_id",
        "as_of_date",
        "asset_symbol",
        "notional_amount",
        "mark_price",
        "classification",
        "quantity",
        "classification_id",
        "financial_value",
        "asset_id",
        "isin",
        "asset_type_id",
        "aquisition_date"
    ]

    df = df[cols]

    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")
    df["aquisition_date"] = pd.to_datetime(df["aquisition_date"], errors="coerce")

    num_cols = ["notional_amount", "mark_price", "quantity", "financial_value"]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    df["snapshot_timestamp"] = datetime.now()

    logging.info("Positions transformation complete.")
    return df


# =============================================================================
# Load
# =============================================================================
def load(df: pd.DataFrame, engine):
    logging.info("Loading into funds_positions_snapshot...")

    df.to_sql(
        "funds_positions_snapshot",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=2000
    )

    logging.info(f"Inserted {len(df)} rows into funds_positions_snapshot.")


# =============================================================================
# Main
# =============================================================================
def main():
    logging.info("Starting pipeline: POSITIONS_SNAPSHOT")

    json_path = "../2-api-simulation/positions_snapshot.json"
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
# Entry point
# =============================================================================
if __name__ == "__main__":
    main()
