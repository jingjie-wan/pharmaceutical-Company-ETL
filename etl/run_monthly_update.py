import argparse
import sqlite3
from pathlib import Path
import pandas as pd

DB_PATH = Path("pharma.db")
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

# Each month, when the new data comes in, we run this script to:
# Load raw csv file(IQVIA data) of given month;
# Clean and normalize data for database;
# Update dimension and fact tables in database;
# Generate file for Tableau dashboard.

def connect_db():
    return sqlite3.connect(DB_PATH)


# ------------------------------
# 1. Dimension tables
# ------------------------------

def upsert_dim_date(conn, month_year: str):
    """write into dim_data"""

    year, month = month_year.split("-")
    date_id = int(year) * 100 + int(month)
    quarter = (int(month) - 1) // 3 + 1
    month_name = pd.to_datetime(month_year).strftime("%B")

    conn.execute(
        """
        INSERT OR REPLACE INTO dim_date(date_id, date, year, quarter, month, month_name, month_year)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            date_id,
            month_year + "-01",
            int(year),
            quarter,
            int(month),
            month_name,
            month_year,
        ),
    )

    return date_id


def upsert_dim_territory(conn, df):
    """write data into dim_territory"""

    rows = df[["territory_code", "state", "region"]].drop_duplicates()

    for row in rows.itertuples(index=False):
        territory_code, state, region = row

        territory_name = f"{state} Territory"
        district = f"{region} District"

        conn.execute(
            """
            INSERT OR IGNORE INTO dim_territory(
                territory_code, territory_name, territory_type,
                district, region, state, territory_manager
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                territory_code,
                territory_name,
                "SAM",
                district,
                region,
                state,
                None,
            ),
        )



def upsert_dim_strength(conn, df):
    """write data into dim_strength"""

    rows = df[["strength_mg"]].drop_duplicates()
    for row in rows.itertuples(index=False):
        (strength,) = row
        conn.execute(
            "INSERT OR IGNORE INTO dim_strength(strength_mg) VALUES (?)",
            (strength,),
        )


def upsert_dim_channel(conn, df):
    """write data into dim_channel"""

    rows = df[["channel_name", "payer_group"]].drop_duplicates()
    for row in rows.itertuples(index=False):
        channel_name, payer_group = row
        conn.execute(
            """
            INSERT OR IGNORE INTO dim_channel(channel_name, payer_group)
            VALUES (?, ?)
            """,
            (channel_name, payer_group),
        )


def upsert_dim_hcp(conn, df):
    """write data into dim_hcp"""

    rows = df[
        [
            "npi_num",
            "first_name",
            "last_name",
            "specialty",
            "territory_code",
            "hcp_segment",
            "is_target",
        ]
    ].drop_duplicates()

    for row in rows.itertuples(index=False):
        (
            npi_num,
            first_name,
            last_name,
            specialty,
            territory_code,
            hcp_segment,
            is_target,
        ) = row

        # search territory_id
        cur = conn.execute(
            "SELECT territory_id FROM dim_territory WHERE territory_code = ?",
            (territory_code,),
        )
        result = cur.fetchone()
        if not result:
            continue
        (territory_id,) = result

        conn.execute(
            """
            INSERT OR IGNORE INTO dim_hcp(
                npi_num, first_name, last_name, specialty,
                territory_id, hcp_segment, is_target,
                new_or_repeat_prescriber, is_new_mot_prescriber
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                npi_num,
                first_name,
                last_name,
                specialty,
                territory_id,
                hcp_segment,
                is_target,
                "New",
                1,
            ),
        )


# ------------------------------
# 2. Fact table
# ------------------------------

def insert_fact_prescriptions(conn, df: pd.DataFrame, date_id: int):
    """
    write data into fact_prescriptions
    """

    # Delete existing records for the month first to avoid duplicates
    conn.execute(
        "DELETE FROM fact_prescriptions WHERE date_id = ?",
        (date_id,),
    )

    for row in df.itertuples(index=False):
        month_year = row.month_year
        npi_num = row.npi_num
        strength_mg = int(row.strength_mg)
        territory_code = row.territory_code
        channel_name = row.channel_name

        trx = int(row.trx)
        nrx = int(row.nrx)
        units = int(row.units)
        net_sales = float(row.net_product_sales)
        conversion_rate = float(row.conversion_rate)
        payer_rate = float(row.payer_reimb_rate)

        # search for foreign keys
        cur = conn.execute(
            "SELECT hcp_id FROM dim_hcp WHERE npi_num = ?", (npi_num,)
        )
        result = cur.fetchone()
        if not result:
            continue
        (hcp_id,) = result

        cur = conn.execute(
            "SELECT strength_id FROM dim_strength WHERE strength_mg = ?",
            (strength_mg,),
        )
        result = cur.fetchone()
        if not result:
            continue
        (strength_id,) = result

        cur = conn.execute(
            "SELECT territory_id FROM dim_territory WHERE territory_code = ?",
            (territory_code,),
        )
        result = cur.fetchone()
        if not result:
            continue
        (territory_id,) = result

        cur = conn.execute(
            "SELECT channel_id FROM dim_channel WHERE channel_name = ?",
            (channel_name,),
        )
        result = cur.fetchone()
        if not result:
            continue
        (channel_id,) = result

        conn.execute(
            """
            INSERT INTO fact_prescriptions(
                date_id, hcp_id, strength_id, territory_id, channel_id,
                trx, nrx, units, net_product_sales,
                conversion_rate, payer_reimb_rate
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date_id,
                hcp_id,
                strength_id,
                territory_id,
                channel_id,
                trx,
                nrx,
                units,
                net_sales,
                conversion_rate,
                payer_rate,
            ),
        )


# ------------------------------
# 3. load wide table for reference
# ------------------------------

def export_tableau_wide_table(conn):
    """
    join star schema into a wide table: data/prescription_kpi_export.csv
    """
    df = pd.read_sql_query(
        """
        SELECT
            d.month_year,
            d.year,
            d.month,

            t.state,
            t.region,
            t.territory_code,
            t.territory_name,
            t.territory_type,
            t.district,

            h.npi_num,
            h.first_name,
            h.last_name,
            h.specialty,
            h.hcp_segment,
            h.is_target,

            s.strength_mg,
            c.channel_name,
            c.payer_group,

            f.trx,
            f.nrx,
            f.units,
            f.net_product_sales,
            f.conversion_rate,
            f.payer_reimb_rate

        FROM fact_prescriptions f
        JOIN dim_date      d ON f.date_id      = d.date_id
        JOIN dim_hcp       h ON f.hcp_id       = h.hcp_id
        JOIN dim_territory t ON f.territory_id = t.territory_id
        JOIN dim_strength  s ON f.strength_id  = s.strength_id
        JOIN dim_channel   c ON f.channel_id   = c.channel_id
        """,
        conn,
    )

    out_path = DATA_DIR / "prescription_kpi_export.csv"
    df.to_csv(out_path, index=False)
    print(f"Exported wide table → {out_path}, shape={df.shape}")


# ------------------------------
# 4. Main ETL flow
# ------------------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", required=True, help="YYYY-MM")
    args = parser.parse_args()

    month = args.month
    raw_path = Path("raw") / f"iqvia_rx_{month}.csv"

    if not raw_path.exists():
        raise FileNotFoundError(f"Raw file not found: {raw_path}")

    print(f"Loading raw file: {raw_path}")
    df = pd.read_csv(raw_path)

    conn = connect_db()

    date_id = upsert_dim_date(conn, month)
    upsert_dim_territory(conn, df)
    upsert_dim_strength(conn, df)
    upsert_dim_channel(conn, df)
    upsert_dim_hcp(conn, df)

    insert_fact_prescriptions(conn, df, date_id)
    conn.commit()

    export_tableau_wide_table(conn)

    conn.close()
    print(f"ETL completed for {month}.")


if __name__ == "__main__":
    main()