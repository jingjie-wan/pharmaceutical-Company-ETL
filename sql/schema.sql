PRAGMA foreign_keys = ON;

-- ========== Dimension Tables ==========

CREATE TABLE dim_date (
    date_id       INTEGER PRIMARY KEY,      -- 20250315
    date          TEXT NOT NULL,           -- '2025-03-31'
    year          INTEGER NOT NULL,
    quarter       INTEGER NOT NULL,
    month         INTEGER NOT NULL,
    month_name    TEXT,
    month_year    TEXT                     -- '2025-03'
);

CREATE TABLE dim_territory (
    territory_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    territory_code    TEXT UNIQUE NOT NULL,   -- 'T001'
    territory_name    TEXT,
    territory_type    TEXT,                   -- SAM / DIRT / Non-Target
    district          TEXT,
    region            TEXT,
    state             TEXT,
    territory_manager TEXT
);

CREATE TABLE dim_strength (
    strength_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    strength_mg   INTEGER UNIQUE NOT NULL          -- 100 / 150 / 200
);

CREATE TABLE dim_channel (
    channel_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_name  TEXT UNIQUE NOT NULL,            -- Commercial / Medicaid / Medicare ...
    payer_group   TEXT                      -- Government/Commercial
);

CREATE TABLE dim_hcp (
    hcp_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    npi_num                 TEXT UNIQUE,
    first_name              TEXT,
    last_name               TEXT,
    specialty               TEXT,           -- Psychiatry/Internal Medicine, ...
    territory_id            INTEGER,
    hcp_segment             TEXT,           -- A/B/C
    is_target               INTEGER,        -- 0/1
    new_or_repeat_prescriber TEXT,          -- 'New' / 'Repeat'
    is_new_mot_prescriber   INTEGER,        -- 0/1
    FOREIGN KEY (territory_id) REFERENCES dim_territory(territory_id)
);

-- ========== Fact Tables ==========

CREATE TABLE fact_prescriptions (
    fact_rx_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id            INTEGER NOT NULL,
    hcp_id             INTEGER,
    strength_id        INTEGER,
    territory_id       INTEGER,
    channel_id         INTEGER,
    trx                INTEGER DEFAULT 0,
    nrx                INTEGER DEFAULT 0,
    units              INTEGER DEFAULT 0,
    net_product_sales  REAL DEFAULT 0.0,
    conversion_rate    REAL,           -- NRx / TRx or payer-specific
    payer_reimb_rate   REAL,           -- optional: reimbursement %
    created_at         TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (date_id)     REFERENCES dim_date(date_id),
    FOREIGN KEY (hcp_id)      REFERENCES dim_hcp(hcp_id),
    FOREIGN KEY (strength_id) REFERENCES dim_strength(strength_id),
    FOREIGN KEY (territory_id)REFERENCES dim_territory(territory_id),
    FOREIGN KEY (channel_id)  REFERENCES dim_channel(channel_id)
);

CREATE TABLE fact_product_metrics (
    fact_pm_id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id                 INTEGER NOT NULL,
    product_name            TEXT NOT NULL,         -- 'Motpoly XR'
    total_trx               INTEGER,
    total_nrx               INTEGER,
    trx_increase_pct_mom    REAL,                  -- MoM
    trx_increase_pct_yoy    REAL,                  -- YoY
    unique_prescribers      INTEGER,
    target_prescribers      INTEGER,
    coverage_all            REAL,                  -- overall coverage rate
    coverage_commercial     REAL,
    coverage_medicaid       REAL,
    coverage_medicare       REAL,
    avg_prescription_value  REAL,
    payer_impact_score      REAL                   -- synthetic index
);

CREATE TABLE fact_shipments (
    fact_ship_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id         INTEGER NOT NULL,
    territory_id    INTEGER,
    shipment_units  INTEGER,
    shipment_value  REAL,
    FOREIGN KEY (date_id)      REFERENCES dim_date(date_id),
    FOREIGN KEY (territory_id) REFERENCES dim_territory(territory_id)
);

CREATE TABLE fact_hcp_calls (
    fact_call_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    date_id         INTEGER NOT NULL,
    hcp_id          INTEGER,
    territory_id    INTEGER,
    call_name       TEXT,         -- e.g. 'Detail', 'Lunch & Learn'
    call_count      INTEGER,
    reach           REAL,         -- %
    adherence       REAL,         -- %
    FOREIGN KEY (date_id)      REFERENCES dim_date(date_id),
    FOREIGN KEY (hcp_id)       REFERENCES dim_hcp(hcp_id),
    FOREIGN KEY (territory_id) REFERENCES dim_territory(territory_id)
);
