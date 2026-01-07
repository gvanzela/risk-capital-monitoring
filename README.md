# Risk Capital Monitoring

End-to-end data engineering project for **risk capital monitoring**, focused on fund exposure, AUM tracking, margin validation, and regulatory risk analytics.

This repository represents a **production-oriented architecture**, rebuilt from a real system and adapted for public demonstration with generic data and endpoints.

---

## Overview

The project models the full lifecycle of a risk capital monitoring platform:

- Data ingestion from multiple sources (APIs, snapshots)
- Consolidation of fund positions, AUM, and margin data
- Exposure and risk calculations
- Snapshot-based historical control
- BI-ready analytical layer

The goal is to demonstrate **how to design, organize, and operate a real data platform**, not just isolated scripts.

---

## Repository Structure

```
risk-capital-monitoring/
│
├── 1-data-model/            # SQL schema and data dictionary
├── 2-api-simulation/        # Mock APIs and sample payloads
├── 3-etl-pipelines/
│   ├── app/                 # Current runtime architecture (active)
│   └── old/                 # Legacy / deprecated ETL code
├── 4-transform/             # SQL transformations and business logic
├── 5-bi/                    # BI layer (mock dashboards, metrics, notes)
│
└── README.md
```

---

## Current Runtime Architecture

The **active execution layer** lives inside:

```
3-etl-pipelines/app/
```

This folder contains the current, production-style architecture.

```
app/
├── main.py      # Orchestration layer (execution order, UI entry point)
├── jobs.py      # Business jobs (ETL, snapshots, validations, backup)
├── config.py    # Environment configuration and constants
├── db.py        # Database engines and connections
├── auth.py      # Authentication helpers
└── __init__.py
```

### Design Principles

- Clear separation of concerns  
- Deterministic and auditable logic  
- Snapshot-based processing (idempotent jobs)  
- Environment-driven configuration (no secrets in code)  
- Safe re-execution of partial pipelines  

---

## Data Model

The data model is intentionally **lean and explicit**, covering core risk entities such as:

- Fund AUM snapshots and history
- Fund positions (OTC, swaps, offshore)
- Risk exposure snapshots
- Manager margin submissions
- Validation logs and exceptions

All tables, columns, and relationships are documented in:

```
1-data-model/
```

---

## ETL and Jobs

The ETL layer is implemented as **explicit jobs**, each responsible for a single concern:

- Margin ingestion
- AUM snapshot
- AUM historical load
- Fund positions ingestion
- Swap processing
- Exposure snapshot reconstruction
- Full remote → local database replication (backup)

Jobs are designed to be:
- Re-runnable
- Batch-consistent
- Independent where possible

---

## Transformations

The `4-transform/` folder centralizes all **SQL-based business logic**, including:

- Exposure consolidation
- Risk aggregation
- Cross-table joins for analytics
- BI-ready views and queries

This layer represents the **analytical backbone** of the platform.

---

## BI Layer

The `5-bi/` folder contains:

- Mock dashboards
- KPI definitions
- Metric explanations
- Notes on analytical design

It mirrors how a real monitoring dashboard would be structured, even when using synthetic data.

---

## Configuration

All credentials, endpoints, and sensitive parameters are provided via environment variables.

A typical setup includes:

- Database connections (remote and local)
- API endpoints
- Authentication tokens
- Runtime parameters (timeouts, retries, SSL flags)

Secrets are intentionally excluded from version control.

---

## Execution

The system can be executed via the main orchestrator:

```bash
python -m app.main
```

This launches the orchestration layer responsible for running individual jobs or full pipelines.

---

## Purpose of This Repository

This project exists to:

**Demonstrate how I design and operate a complete risk data platform — from ingestion to analytics — using clean, structured, and production-grade engineering practices.**

It is not a toy example, nor a full production dump, but a **faithful architectural representation**.

---

## Disclaimer

All data, endpoints, and identifiers in this repository are **generic or simulated**.

This repository is intended for **technical demonstration and architectural discussion only**.
