import sqlite3
from pathlib import Path

DB_PATH = Path("pharma.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

date_id = 202401  # YYYYMM

# 看看当前有多少行
cur.execute("SELECT COUNT(*) FROM fact_prescriptions WHERE date_id = ?", (date_id,))
before_count = cur.fetchone()[0]
print(f"Rows for date_id={date_id} BEFORE delete: {before_count}")

# 删除 2024-01 的所有 fact 记录
cur.execute("DELETE FROM fact_prescriptions WHERE date_id = ?", (date_id,))
conn.commit()

cur.execute("SELECT COUNT(*) FROM fact_prescriptions WHERE date_id = ?", (date_id,))
after_count = cur.fetchone()[0]
print(f"Rows for date_id={date_id} AFTER delete: {after_count}")

conn.close()
print("Done.")
