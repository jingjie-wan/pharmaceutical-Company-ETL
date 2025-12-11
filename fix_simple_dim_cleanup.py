import sqlite3
from pathlib import Path

DB_PATH = Path("pharma.db")

def cleanup_simple(conn):
    cur = conn.cursor()

    print("=== Cleaning dim_strength (keep first 3 rows) ===")

    # 获取前 3 个 id
    cur.execute("SELECT strength_id FROM dim_strength ORDER BY strength_id LIMIT 3")
    keep_strength_ids = [row[0] for row in cur.fetchall()]
    print("Keeping:", keep_strength_ids)

    # 删除其余所有行
    cur.execute(
        f"""
        DELETE FROM dim_strength
        WHERE strength_id NOT IN ({','.join('?' * len(keep_strength_ids))})
        """,
        keep_strength_ids
    )
    conn.commit()

    # 统计剩余行
    cur.execute("SELECT COUNT(*) FROM dim_strength")
    print("Remaining dim_strength rows:", cur.fetchone()[0])

    print("\n=== Cleaning dim_channel (keep first 4 rows) ===")

    # 获取前 4 行的 id
    cur.execute("SELECT channel_id FROM dim_channel ORDER BY channel_id LIMIT 4")
    keep_channel_ids = [row[0] for row in cur.fetchall()]
    print("Keeping:", keep_channel_ids)

    # 删除其余所有行
    cur.execute(
        f"""
        DELETE FROM dim_channel
        WHERE channel_id NOT IN ({','.join('?' * len(keep_channel_ids))})
        """,
        keep_channel_ids
    )
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM dim_channel")
    print("Remaining dim_channel rows:", cur.fetchone()[0])

    print("\nDone.")


def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"{DB_PATH} not found")

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("PRAGMA foreign_keys = OFF;")  # 重要：避免因 fact 中的旧引用报错
        cleanup_simple(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
