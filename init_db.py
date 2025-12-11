import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent
db_path = ROOT / "pharma.db"
schema_path = ROOT / "sql" / "schema.sql"

print("Using DB path:", db_path)
print("Using schema path:", schema_path)

# 1. 删除旧库，保证是全新创建
if db_path.exists():
    print("Deleting existing pharma.db ...")
    db_path.unlink()

# 2. 连接并执行 schema.sql
if not schema_path.exists():
    raise FileNotFoundError(f"schema.sql not found at: {schema_path}")

print("schema.sql size (bytes):", schema_path.stat().st_size)

with open(schema_path, "r", encoding="utf-8") as f:
    script = f.read()

print("\n=== First 200 chars of schema.sql ===")
print(script[:200])
print("=== End preview ===\n")

conn = sqlite3.connect(db_path)
conn.executescript(script)
conn.commit()
print("Executed schema.sql.\n")

# 3. 打印所有表和建表语句
cur = conn.cursor()
cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table' ORDER BY name;")
rows = cur.fetchall()

print("Tables in pharma.db:")
if not rows:
    print("  (No tables found!)")
else:
    for name, sql in rows:
        print(f"- {name}")
        # 只print前80字符，避免太长
        if sql:
            print("  SQL:", sql[:80].replace("\n", " "), "...")
        else:
            print("  (no SQL text)")

conn.close()
print("\nDone.")
