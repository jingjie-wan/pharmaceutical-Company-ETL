import subprocess

def month_range(start, end):
    sy, sm = map(int, start.split("-"))
    ey, em = map(int, end.split("-"))
    y, m = sy, sm
    while (y < ey) or (y == ey and m <= em):
        yield f"{y:04d}-{m:02d}"
        m += 1
        if m > 12:
            m = 1
            y += 1

def main():
    for month in month_range("2023-01", "2025-12"):
        print(f"\n=== Running ETL for {month} ===")
        subprocess.run(
            ["python", "etl/run_monthly_update.py", "--month", month],
            check=True
        )

if __name__ == "__main__":
    main()
