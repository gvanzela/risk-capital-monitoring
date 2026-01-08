# Data Dictionary — Risk Capital Data Platform

This document describes the **intentionally designed data model** used by the Risk Capital monitoring platform.

All tables, columns, and relationships were **explicitly modeled** to support operational risk monitoring, auditability, and analytical consumption.  
Although data is ingested from APIs and external systems, the **schema itself is authored and controlled**, not inferred or reverse-engineered.

The platform follows an **append-only**, **audit-friendly** design, prioritizing traceability, historical replay, and analytical reliability.

---

## Overview of Tables

| Table | Description |
|------|-------------|
| `TB_ENQ_MARGEM_GESTOR_SNAPSHOT` | Daily snapshot of margin metrics by manager |
| `TB_ENQ_PL_SNAPSHOT` | Daily snapshot of fund-level PL and risk indicators |
| `TB_ENQ_PL_HISTORICO` | Historical daily PL by fund |
| `TB_ENQ_POSICOES_FUNDOS_EXPOSTOS` | Detailed fund positions (OTC, Offshore, Swaps) |
| `TB_ENQ_EXPOSI_RISCO_SNAPSHOT` | Snapshot of funds exposed by risk origin |

---

## TB_ENQ_MARGEM_GESTOR_SNAPSHOT

**Description**  
Stores daily margin snapshots by manager.  
Data is ingested from API queries, but the table structure and semantics are explicitly defined by the platform.

### Columns

| Column | Description |
|------|------------|
| `DataEnvio` | Reference date of the margin data |
| `MargemLocal` | Local margin amount |
| `MargemOffshore` | Offshore margin amount |
| `dt_carga` | Load timestamp identifying the execution batch |

### Keys
- Logical uniqueness: `DataEnvio`, `dt_carga`
- Physical primary key: intentionally not enforced

---

## TB_ENQ_PL_SNAPSHOT

**Description**  
Daily snapshot of fund-level PL, liquidity, derivatives exposure, and risk metrics.  
Each execution produces a complete snapshot for analytical consistency.

### Main Columns

| Column | Description |
|------|------------|
| `cgePortfolio` | Fund identifier |
| `nomeFundo` | Fund name |
| `cnpj` | Fund tax identifier |
| `nomeGestor` | Fund manager |
| `pl` | Net asset value |
| `margemPl` | Margin relative to PL |
| `derivativosPl` | Derivatives exposure relative to PL |
| `liquidezPl` | Liquidity ratio |
| `var95`, `var99` | Value-at-Risk metrics |
| `criticidade` | Risk classification |
| `dt_carga` | Load timestamp identifying the execution batch |

### Keys
- Logical uniqueness: `cgePortfolio`, `dt_carga`
- Physical primary key: intentionally not enforced

---

## TB_ENQ_PL_HISTORICO

**Description**  
Stores the historical daily PL evolution for each fund, preserving one record per fund per reference date.

### Columns

| Column | Description |
|------|------------|
| `cgePortfolio` | Fund identifier |
| `data` | Portfolio reference date |
| `patrimonio_abertura` | Opening PL |
| `patrimonio_fechamento` | Closing PL |
| `dt_carga` | Load timestamp |

### Keys
- Logical primary key: (`cgePortfolio`, `data`)
- Designed to be stable and replayable

---

## TB_ENQ_POSICOES_FUNDOS_EXPOSTOS

**Description**  
Core positions table containing detailed exposures for OTC, Offshore, and Swap instruments.

Each execution inserts a **full, timestamped snapshot**, enabling safe reprocessing and historical reconstruction.

### Main Columns

| Column | Description |
|------|------------|
| `Nickname` | Asset identifier |
| `DataCarteira` | Position reference date |
| `CgePortfolio` | Fund identifier |
| `notional` | Notional amount |
| `valorfinanceiro` | Financial value |
| `qtyposicao` | Position quantity |
| `NmClassificacao` | Exposure type (OTC, Offshore, Swap) |
| `NuIsin` | ISIN code |
| `CodAtivo` | Asset code |
| `CodTipoAtivo` | Asset type code |
| `dt_carteira` | Portfolio reference date |
| `dt_insercao` | Execution batch timestamp |

### Keys
- Batch identifier: `dt_insercao`
- Logical uniqueness within batch:  
  `CgePortfolio`, `Nickname`, `DataCarteira`, `dt_insercao`
- Physical primary key: intentionally not enforced (append-only strategy)

---

## TB_ENQ_EXPOSI_RISCO_SNAPSHOT

**Description**  
Derived snapshot identifying which funds are exposed to each risk origin, based on the **latest complete positions batch**.

This table is rebuilt per execution to guarantee batch-level consistency.

### Columns

| Column | Description |
|------|------------|
| `cgePortfolio` | Fund identifier |
| `origem` | Exposure origin |
| `dt_carga` | Load timestamp matching the source batch |

### Keys
- Logical uniqueness: `cgePortfolio`, `dt_carga`
- Designed for idempotent reconstruction

---

## Design Notes

- The data model is **explicitly designed**, not inferred
- All tables follow an **append-only** strategy
- No hard primary keys are enforced by design
- Consistency is guaranteed via:
  - reference dates
  - execution timestamps (`dt_carga`, `dt_insercao`)
- The model prioritizes:
  - auditability
  - historical replay
  - analytical and regulatory workloads

---

## Source of Truth

This data dictionary reflects the **authoritative data model implemented by the platform**, as defined in:

- `jobs.py`
- `app/main.py`

The schema is authored intentionally to support the operational and analytical requirements of risk capital monitoring.
