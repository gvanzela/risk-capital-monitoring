# Data Dictionary  
_Comprehensive documentation for the analytical data model used in the Risk Capital Monitoring System._

---

## 1. FUNDS_AUM_SNAPSHOT  
**Description:** Latest available AUM (Assets Under Management) per fund.

| Generic Column       | Type      | Description                               |
|----------------------|-----------|-------------------------------------------|
| fund_id              | INT       | Unique identifier for the fund           |
| fund_name            | TEXT      | Fund name                                 |
| fund_class           | TEXT      | CVM classification / fund strategy        |
| manager_name         | TEXT      | Name of the portfolio manager             |
| investor_type        | TEXT      | Target investor segment                   |
| aum_value            | DECIMAL   | Total net assets (AUM)                    |
| snapshot_timestamp   | DATETIME  | Ingestion timestamp                        |

---

## 2. FUNDS_AUM_HISTORY  
**Description:** Monthly AUM evolution (opening/closing values by fund).

| Generic Column       | Type      | Description                                    |
|----------------------|-----------|------------------------------------------------|
| fund_id              | INT       | Fund identifier                                |
| reference_date       | DATE      | Month reference date                           |
| aum_open_value       | DECIMAL   | Opening AUM for the month                      |
| aum_close_value      | DECIMAL   | Closing AUM for the month                      |
| snapshot_timestamp   | DATETIME  | Ingestion timestamp                            |

---

## 3. FUNDS_POSITIONS_SNAPSHOT  
**Description:** Full portfolio composition snapshot (per fund per date).

| Generic Column       | Type      | Description                                      |
|----------------------|-----------|--------------------------------------------------|
| fund_id              | INT       | Fund identifier                                  |
| portfolio_date       | DATE      | Portfolio reference date                         |
| notional_value       | DECIMAL   | Gross notional of the position                   |
| price_value          | DECIMAL   | Market price used                                |
| quantity             | DECIMAL   | Position size / quantity                         |
| market_value         | DECIMAL   | Mark-to-market value (qty × price)               |
| asset_id             | INT       | Internal asset identifier                         |
| isin_code            | TEXT      | ISIN                                              |
| asset_type_code      | INT       | Asset class/type code                             |
| asset_alias          | TEXT      | Short asset name                                  |
| asset_category       | TEXT      | Category (equity, FI, derivatives, offshore…)    |
| snapshot_timestamp   | DATETIME  | Ingestion timestamp                               |

---

## 4. RISK_EXPOSURE_SNAPSHOT  
**Description:** Exposure type (OTC, SWAP, Offshore) for each fund.

| Generic Column       | Type      | Description                            |
|----------------------|-----------|----------------------------------------|
| fund_id              | INT       | Fund identifier                        |
| exposure_origin      | TEXT      | Exposure type (OTC, SWAP, Offshore)    |
| reference_date       | DATE      | Exposure date                           |
| snapshot_timestamp   | DATETIME  | Ingestion timestamp                     |

---

## 5. MANAGER_MARGIN_SNAPSHOT  
**Description:** Daily margin submission from portfolio managers.

| Generic Column       | Type      | Description                            |
|----------------------|-----------|----------------------------------------|
| fund_id              | INT       | Fund identifier                        |
| submission_date      | DATE      | Date the manager submitted the margin |
| margin_local         | DECIMAL   | Local market margin                    |
| margin_offshore      | DECIMAL   | Offshore margin                        |
| snapshot_timestamp   | DATETIME  | Ingestion timestamp                    |

---

## 6. MARGIN_VALIDATION_LOG  
**Description:** Internal validation log for submitted margins.

| Generic Column       | Type      | Description                                   |
|----------------------|-----------|-----------------------------------------------|
| fund_id              | INT       | Fund identifier                               |
| validation_date      | DATE      | Date the validation was performed            |
| validation_flag      | TINYINT   | 1 = validated, 0 = rejected                   |
| snapshot_timestamp   | DATETIME  | Ingestion timestamp                           |

---

## 7. MARGIN_EXCEPTIONS  
**Description:** Exceptions or overrides applied to margin submissions.

| Generic Column       | Type      | Description                           |
|----------------------|-----------|---------------------------------------|
| fund_id              | INT       | Fund identifier                        |
| exception_flag       | TINYINT   | Override indicator (1 = exception)     |

---

## 8. ASSET_TYPE_MAPPING  
**Description:** Mapping between asset type codes and readable descriptions.

| Generic Column       | Type      | Description                                     |
|----------------------|-----------|-------------------------------------------------|
| asset_type_code      | INT       | Numeric type code                               |
| asset_type_desc      | TEXT      | Human-readable description (e.g., Option, FI)   |

---

## Notes
- All timestamps refer to ingestion time, not portfolio or market timestamps.  
- The model is normalized but intentionally simple to make the project easy to understand and reproduce.  
- Column names follow a consistent naming convention across tables.

