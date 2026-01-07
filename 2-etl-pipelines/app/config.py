import os
from dotenv import load_dotenv

# Carrega .env da raiz do projeto
load_dotenv()

# =========================
# DATABASE — REMOTE
# =========================
DB_REMOTE_USER = os.getenv("DB_REMOTE_USER")
DB_REMOTE_PASS = os.getenv("DB_REMOTE_PASS")
DB_REMOTE_HOST = os.getenv("DB_REMOTE_HOST")
DB_REMOTE_PORT = os.getenv("DB_REMOTE_PORT")
DB_REMOTE_NAME = os.getenv("DB_REMOTE_NAME")

# =========================
# DATABASE — LOCAL
# =========================
DB_LOCAL_USER = os.getenv("DB_LOCAL_USER")
DB_LOCAL_PASS = os.getenv("DB_LOCAL_PASS")
DB_LOCAL_HOST = os.getenv("DB_LOCAL_HOST")
DB_LOCAL_PORT = os.getenv("DB_LOCAL_PORT")
DB_LOCAL_NAME = os.getenv("DB_LOCAL_NAME")

# =========================
# METABASE
# =========================
METABASE_BASE = os.getenv("METABASE_BASE")
METABASE_USER = os.getenv("METABASE_USER")
METABASE_PASS = os.getenv("METABASE_PASS")

METABASE_PUBLIC_CARD_ENDPOINT = os.getenv("METABASE_PUBLIC_CARD_ENDPOINT")

METABASE_CARD_MARGEM = os.getenv("METABASE_CARD_MARGEM")
METABASE_CARD_PL_HIST = os.getenv("METABASE_CARD_PL_HIST")
METABASE_CARD_POSICOES_OTC = os.getenv("METABASE_CARD_POSICOES_OTC")
METABASE_CARD_POSICOES_SWAP = os.getenv("METABASE_CARD_POSICOES_SWAP")
METABASE_CARD_POSICOES_OFF = os.getenv("METABASE_CARD_POSICOES_OFF")
METABASE_CARD_POSICOES_PUBLIC = os.getenv("METABASE_CARD_POSICOES_PUBLIC")

# =========================
# FUNDS PLATFORM
# =========================
FUNDS_API_BASE = os.getenv("FUNDS_API_BASE")
FUNDS_API_ENDPOINT_PL = os.getenv("FUNDS_API_ENDPOINT_PL")
FUNDS_API_COOKIE = os.getenv("FUNDS_API_COOKIE")
FUNDS_API_CRYPTO_TOKEN = os.getenv("FUNDS_API_CRYPTO_TOKEN")

# =========================
# RUNTIME
# =========================
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", 300))
REQUEST_RETRY_SLEEP = float(os.getenv("REQUEST_RETRY_SLEEP", 0.5))
VERIFY_SSL = os.getenv("VERIFY_SSL", "false").lower() == "true"
TIMEZONE = os.getenv("TIMEZONE", "BRT")
