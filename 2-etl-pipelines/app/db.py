from sqlalchemy import create_engine
from app.config import (
    DB_REMOTE_USER, DB_REMOTE_PASS, DB_REMOTE_HOST, DB_REMOTE_PORT, DB_REMOTE_NAME,
    DB_LOCAL_USER, DB_LOCAL_PASS, DB_LOCAL_HOST, DB_LOCAL_PORT, DB_LOCAL_NAME
)

def _build_mysql_url(user, password, host, port, db):
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"

# Engine — REMOTE
ENGINE_REMOTE = create_engine(
    _build_mysql_url(
        DB_REMOTE_USER,
        DB_REMOTE_PASS,
        DB_REMOTE_HOST,
        DB_REMOTE_PORT,
        DB_REMOTE_NAME
    ),
    pool_pre_ping=True
)

# Engine — LOCAL
ENGINE_LOCAL = create_engine(
    _build_mysql_url(
        DB_LOCAL_USER,
        DB_LOCAL_PASS,
        DB_LOCAL_HOST,
        DB_LOCAL_PORT,
        DB_LOCAL_NAME
    ),
    pool_pre_ping=True
)
