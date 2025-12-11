# Motpoly Monthly Pharma ETL pipeline & Dashboard Analytics  
_End-to-end data pipeline, analytics modeling & Power BI dashboard_

## 1. Project Overview

This project simulates a **pharmaceutical analytics workflow** for medicine, **Motpoly XR**, covering U.S. prescription activity from **2023–2025**. The process is the same in real-world pharmaceutical companies. However, for security reasons, I built the data and system by myself to replicate the process.

This project solves the problem of manually piecing together monthly prescription data by providing an automated end-to-end pipeline—from raw data ingestion to analytics dashboards. It built a scalable, structured, and clean star-schema data warehouse and a monthly updates and refreshing solution. The Power BI dashboards then help brand and commercial teams quickly identify perscription and sales performance changes and help spot the reason. This turns raw data into a repeatable, scalable insight-generation system.


It contains:

- Design and build a **SQL analytical database** (SQLite)
- Generate **synthetic IQVIA-style monthly feeds**
- Build a **Python ETL pipeline** that performs monthly incremental updates
- Produce **actionable Power BI dashboards** with KPI logic, MoM arrows, territory insights, and payer analysis
- Implement a professional **end-to-end** monthly refresh process

**Pipeline Architecture:**

```text
Build SQL database (SQLite)
        ↓
Synthetic RAW monthly CSVs (IQVIA-like feeds)
        ↓
Python ETL monthly update (incremental load)
        ↓
Connect to Power BI → Monthly Refresh Power BI Report
```
## 2. Tech Stack

- **Python** (pandas, sqlite3, Faker, numpy)
- **SQLite** (embedded analytical DB)
- **Power BI** (including DAX)

## 3. Data Model (Star Schema)
<img width="600" height="800" alt="image" src="https://github.com/user-attachments/assets/efd93bdd-9f47-4578-9d3a-22d082a68f93" />



### Fact Table — `fact_prescriptions`

Contains one row per HCP × Territory × Month × Payer combination.

Key metrics:

- `trx` — total prescriptions  
- `nrx` — new prescriptions  
- `units`  
- `net_product_sales`
- `payer`
- `channel`
- `payer_reimb_rate`
- `conversion_rate`
, Etc. 
---

### Dimension Tables

| Table           | Purpose                                                                 |
|-----------------|-------------------------------------------------------------------------|
| `dim_date`      | Calendar hierarchy                               |
| `dim_hcp`       | Prescriber info (name, specialty, etc.)                                      |
| `dim_territory` | State-level territory + region (Midwest / Northeast / South / West)    |
| `dim_payer`     | Payer group (Commercial / Government), reimbursement band              |
| `dim_channel`   | Retail / Specialty / Mail Order                                        |
| `dim_strength`  | Motpoly strength (100mg, 150mg, 200mg)                                 |

This schema supports flexible slicing for TRx, NRx, sales, payer band performance, and geographic comparisons.

---

## 4. Environment Setup

    git clone <repo-url>
    cd pharma-monthly-update

    pip install -r requirements.txt

---

## 5. Build the SQLite Database

Run:

    python sql/init_db.py

This will:

- Remove any existing `pharma.db`
- Execute all DDL in `schema.sql`
- Confirm all tables were created successfully

You could also ensure the database is created successfully by running `check_db.py`



---

## 6. Generate Synthetic IQVIA-style Monthly Data

    python etl/generate_synthetic_raw.py

This script creates monthly files in `raw/`:

    raw/iqvia_rx_2023-01.csv
    raw/iqvia_rx_2023-02.csv
    ...
    raw/iqvia_rx_2025-12.csv


---

## 7. Run Monthly Update

To update the database with the lastest data:

    python etl/run_monthly_update.py --month 2024-01

The ETL performs:

1. Load raw CSV for the specified month  
2. Clean and validate  
3. Upsert dimension tables  
4. Insert/overwrite fact data for that month 
6. Generate/update `data/prescription_kpi_export.csv` for Power BI  

---

## 9. Power BI Dashboard

The Power BI report is connected either to:

- `pharma.db` (via SQLite connector)

There are **two pages**:

---

### Page 1 — Motpoly Monthly Summary

**Purpose:** Executive snapshot for brand performance for executives.
<img width="1680" height="942" alt="image" src="https://github.com/user-attachments/assets/0239e596-ccf5-4d6d-ac41-a95b547c5884" />





This page answers:

> “How is the brand performing overall, and what drove the recent month’s change?”

---

### Page 2 — Territory & Geographic Deep Dive

**Purpose:** Diagnose *where* performance shifts occur geographically.
<img width="1659" height="925" alt="image" src="https://github.com/user-attachments/assets/c3739b3f-4c0b-4001-b489-a8737d357241" />


This page allows teams to answer:

> “Which regions and states are causing the monthly change?”

> “Where are HCPs underperforming?”  

> “Which territories need sales and marketing intervention?”

---
Enjoy!


