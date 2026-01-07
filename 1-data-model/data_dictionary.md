# Data Dictionary — Risk Capital Data Platform

This document describes the database tables produced and consumed by the Risk Capital automation jobs.  
The schema and definitions below are derived directly from the implementation (`jobs.py`) and reflect the actual operational data model.

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
Stores daily margin snapshots by manager, sourced from Metabase queries.

### Columns

| Column | Description |
|------|------------|
| `DataEnvio` | Reference date of the margin data |
| `MargemLocal` | Local margin amount |
| `MargemOffshore` | Offshore margin amount |
| `dt_carga` | Load timestamp |

### Keys
- Natural key (inferred): `DataEnvio`, `dt_carga`
- Primary key: not explicitly defined

---

## TB_ENQ_PL_SNAPSHOT

**Description**  
Daily snapshot of fund-level PL, liquidity, derivatives exposure, and risk metrics.

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
| `dt_carga` | Load timestamp |

### Keys
- Natural key (inferred): `cgePortfolio`, `dataProcessamento`
- Primary key: not explicitly defined

---

## TB_ENQ_PL_HISTORICO

**Description**  
Stores the historical daily PL evolution for each fund.

### Columns

| Column | Description |
|------|------------|
| `cgePortfolio` | Fund identifier |
| `data` | Portfolio reference date |
| `patrimonio_abertura` | Opening PL |
| `patrimonio_fechamento` | Closing PL |
| `dt_carga` | Load timestamp |

### Keys
- Natural key: `cgePortfolio`, `data`
- Suggested primary key: (`cgePortfolio`, `data`)

---

## TB_ENQ_POSICOES_FUNDOS_EXPOSTOS

**Description**  
Core positions table containing detailed exposures for OTC, Offshore, and Swap instruments.  
Each execution inserts a full, timestamped snapshot of positions.

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
| `dt_carteira` | Portfolio date |
| `dt_insercao` | Batch insertion timestamp |

### Keys
- Batch identifier: `dt_insercao`
- Natural composite key (inferred):  
  `CgePortfolio`, `Nickname`, `DataCarteira`, `dt_insercao`
- Primary key: not explicitly defined (append-only design)

---

## TB_ENQ_EXPOSI_RISCO_SNAPSHOT

**Description**  
Derived snapshot identifying which funds are exposed to each risk origin, based on the latest positions load.

### Columns

| Column | Description |
|------|------------|
| `cgePortfolio` | Fund identifier |
| `origem` | Exposure origin |
| `dt_carga` | Load timestamp |

### Keys
- Natural key: `cgePortfolio`, `dt_carga`
- Suggested primary key: (`cgePortfolio`, `dt_carga`)

---

## Design Notes

- All tables follow an **append-only** strategy
- No hard primary keys are enforced at the database level
- Data integrity relies on:
  - reference dates
  - load timestamps (`dt_carga`, `dt_insercao`)
- The model is optimized for:
  - auditability
  - historical reconstruction
  - analytical workloads (BI / risk monitoring)

---

## Source

This data dictionary was derived directly from the production codebase:
- `jobs.py`
- `app.py`

No external assumptions or manual schema adjustments were applied.
