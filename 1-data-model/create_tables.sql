-- Data Model (MySQL)
-- Minimal schema for the Risk Capital Monitoring project

CREATE TABLE IF NOT EXISTS FUNDS_AUM_SNAPSHOT (
    fund_id             INT             NOT NULL,
    as_of_date          DATE            NOT NULL,
    fund_name           VARCHAR(255),
    fund_cnpj           VARCHAR(32),
    manager_name        VARCHAR(255),
    investor_type       VARCHAR(100),
    portfolio_type      VARCHAR(100),
    fund_class          VARCHAR(100),
    fund_class_desc     VARCHAR(255),

    aum_value           DECIMAL(20,6),

    liquidity_ratio     DECIMAL(20,6),
    return_daily        DECIMAL(20,6),
    return_monthly      DECIMAL(20,6),
    return_ytd          DECIMAL(20,6),

    risk_rating         VARCHAR(50),
    var_95              DECIMAL(20,6),
    var_99              DECIMAL(20,6),

    snapshot_timestamp  DATETIME        NOT NULL
);

CREATE TABLE FUNDS_AUM_HISTORY (
    fund_id INT,
    reference_date DATE,
    aum_open_value DECIMAL(20,2),
    aum_close_value DECIMAL(20,2),
    snapshot_timestamp DATETIME
);

CREATE TABLE FUNDS_POSITIONS_SNAPSHOT (
    fund_id INT,
    portfolio_date DATE,
    notional_value DECIMAL(20,6),
    price_value DECIMAL(20,6),
    quantity DECIMAL(20,6),
    asset_class_id INT,
    market_value DECIMAL(20,6),
    asset_id INT,
    isin_code VARCHAR(50),
    asset_type_code INT,
    asset_alias VARCHAR(200),
    asset_category VARCHAR(200),
    snapshot_timestamp DATETIME
);

CREATE TABLE RISK_EXPOSURE_SNAPSHOT (
    fund_id INT,
    exposure_origin VARCHAR(100),
    reference_date DATE,
    snapshot_timestamp DATETIME
);

CREATE TABLE MANAGER_MARGIN_SNAPSHOT (
    fund_id INT,
    submission_date DATE,
    margin_local DECIMAL(20,6),
    margin_offshore DECIMAL(20,6),
    snapshot_timestamp DATETIME
);

CREATE TABLE MARGIN_VALIDATION_LOG (
    fund_id INT,
    validation_date DATE,
    validation_flag TINYINT,
    snapshot_timestamp DATETIME
);

CREATE TABLE MARGIN_EXCEPTIONS (
    fund_id INT,
    exception_flag TINYINT
);

CREATE TABLE ASSET_TYPE_MAPPING (
    asset_type_code INT,
    asset_type_desc VARCHAR(200)
);

