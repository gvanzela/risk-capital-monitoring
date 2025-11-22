"""
ETL Pipeline — Funds AUM Snapshot
---------------------------------
Loads the daily fund snapshot (AUM + risk metrics) from a simulated API
and inserts it into the FUNDS_AUM_SNAPSHOT table.

This version includes a fake API fetch layer, simulating latency,
validation and request behavior — ideal for portfolio demonstration.
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
    In production this would load credentials from environment variables
    or a secrets manager. For portfolio purposes, we keep it simple.
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
    Simulates an API request:
    - validates the file path
    - adds artificial network latency
    - returns parsed JSON

    This function replaces a real `requests.get()` call, keeping the
    repository safe and generic.
    """
    logging.info("Simulating API request...")

    # simulate network delay
    time.sleep(0.8)

    if not os.path.exists(json_path):
        raise FileNotFoundError(f"Simulated API file not found: {json_path}")

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    logging.info("API response successfully retrieved from simulation.")
    return data


# =============================================================================
# Extract
# =============================================================================
def extract(json_path: str) -> pd.DataFrame:
    logging.info(f"Extracting data from: {json_path}")

    data = fetch_fake_api(json_path)
    df = pd.DataFrame(data)

    logging.info(f"Extracted {len(df)} rows from API simulation.")
    return df


# =============================================================================
# Transform
# =============================================================================
def transform(df: pd.DataFrame) -> pd.DataFrame:
    logging.info("Transforming dataset...")

    rename_map = {
        "cgePortfolio": "fund_id",
        "dataPosicao": "as_of_date",
        "nomeFundo": "fund_name",
        "cnpj": "fund_cnpj",
        "nomeGestor": "manager_name",
        "publicoAlvo": "investor_type",
        "tipoPortfolio": "portfolio_type",
        "classeCvm": "fund_class",
        "descClasseCvm": "fund_class_desc",
        "pl": "aum_value",
        "liquidezPl": "liquidity_ratio",
        "rentabilidadeDia": "return_daily",
        "rentabilidadeMes": "return_monthly",
        "rentabilidadeAno": "return_ytd",
        "criticidade": "risk_rating",
        "var95": "var_95",
        "var99": "var_99",
    }

    df = df.rename(columns=rename_map)

    final_cols = [
        "fund_id",
        "as_of_date",
        "fund_name",
        "fund_cnpj",
        "manager_name",
        "investor_type",
        "portfolio_type",
        "fund_class",
        "fund_class_desc",
        "aum_value",
        "liquidity_ratio",
        "return_daily",
        "return_monthly",
        "return_ytd",
        "risk_rating",
        "var_95",
        "var_99"
    ]

    df = df[final_cols]

    # type casting
    df["as_of_date"] = pd.to_datetime(df["as_of_date"], errors="coerce")

    numeric_cols = [
        "aum_value",
        "liquidity_ratio",
        "return_daily",
        "return_monthly",
        "return_ytd",
        "var_95",
        "var_99"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["snapshot_timestamp"] = datetime.now()

    logging.info("Transformation completed.")
    return df


# =============================================================================
# Load
# =============================================================================
def load(df: pd.DataFrame, engine):
    logging.info("Loading dataset into FUNDS_AUM_SNAPSHOT...")

    df.to_sql(
        "FUNDS_AUM_SNAPSHOT",
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
    logging.info("Starting pipeline: FUNDS_AUM_SNAPSHOT")

    json_path = "../2-api-simulation/funds_aum_snapshot.json"
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

