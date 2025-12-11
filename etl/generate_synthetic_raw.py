# etl/generate_synthetic_raw.py
import argparse
from pathlib import Path
import random

import numpy as np
import pandas as pd
from faker import Faker

RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)
fake = Faker("en_US")

RAW_DIR = Path("raw")

# 全 50 州（不含 DC）
US_STATES = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
]

REGION_BY_STATE = {
    # Northeast
    "ME":"Northeast","NH":"Northeast","VT":"Northeast","MA":"Northeast",
    "RI":"Northeast","CT":"Northeast","NY":"Northeast","NJ":"Northeast",
    "PA":"Northeast",
    # Midwest
    "OH":"Midwest","MI":"Midwest","IN":"Midwest","IL":"Midwest","WI":"Midwest",
    "MN":"Midwest","IA":"Midwest","MO":"Midwest","ND":"Midwest","SD":"Midwest",
    "NE":"Midwest","KS":"Midwest",
    # South
    "DE":"South","MD":"South","VA":"South","WV":"South",
    "NC":"South","SC":"South","GA":"South","FL":"South",
    "KY":"South","TN":"South","AL":"South","MS":"South",
    "AR":"South","LA":"South","OK":"South","TX":"South",
    # West
    "MT":"West","WY":"West","CO":"West","NM":"West","AZ":"West",
    "UT":"West","NV":"West","ID":"West","WA":"West","OR":"West",
    "CA":"West","AK":"West","HI":"West",
}


def month_multiplier(month_str: str) -> float:
    """7 / 8 月整体水平下滑（dip），其它月份略微上升。"""
    year, m = map(int, month_str.split("-"))
    if m in (7, 8):
        return 0.6                      # 明显谷底
    base = 1.0 + 0.03 * (m - 1)         # 其他月份缓慢增长
    return base

def generate_iqvia_rx(month: str, hcps_per_state: int = 30):
    """
    为指定 month（YYYY-MM）生成一份 raw/iqvia_rx_YYYY-MM.csv

    ✅ 每个州都有 hcps_per_state 个 HCP 记录
    ✅ 7、8 月有明显 dip（通过 month_multiplier 控制）
    """
    RAW_DIR.mkdir(exist_ok=True)

    specialties = ["Psychiatry", "Internal Medicine", "Neurology"]
    segments = ["A", "B", "C"]
    channels = ["Commercial", "Medicaid", "Medicare", "Managed Medicaid"]
    payer_group_map = {
        "Commercial": "Commercial",
        "Medicaid": "Government",
        "Medicare": "Government",
        "Managed Medicaid": "Government",
    }
    strengths = [100, 150, 200]

    mm = month_multiplier(month)
    rows = []

    for s_idx, state in enumerate(US_STATES):
        for h_idx in range(hcps_per_state):
            # 对同一州、同一下标的 HCP，各个月 NPI 保持一致
            npi = f"{s_idx:02d}{h_idx:07d}"  # 9 位数字

            profile = fake.simple_profile()
            first = profile["name"].split(" ")[0]
            last = profile["name"].split(" ")[-1]

            specialty = random.choice(specialties)
            segment = random.choice(segments)
            is_target = 1 if segment in ["A", "B"] else 0
            territory_code = f"{state}01"   # e.g. 'NY01'
            region = REGION_BY_STATE.get(state, "Other")

            channel = random.choice(channels)
            payer_group = payer_group_map[channel]
            strength = random.choice(strengths)

            # 基础 TRx 水平：target HCP 高一些
            base_trx = np.random.normal(18 if is_target else 6, 5)
            base_trx = max(0, base_trx)

            # 叠加月份因子（7/8 月整体更低）
            trx = int(base_trx * mm)
            trx = max(trx, 0)

            nrx = int(trx * np.random.uniform(0.25, 0.55))
            units = trx * random.choice([1, 2])
            net_sales = units * random.uniform(220, 480)
            conversion_rate = nrx / trx if trx > 0 else 0
            payer_reimb_rate = random.uniform(0.5, 0.85)

            rows.append(
                {
                    "month_year": month,
                    "npi_num": npi,
                    "first_name": first,
                    "last_name": last,
                    "specialty": specialty,
                    "state": state,
                    "territory_code": territory_code,
                    "region": region,
                    "hcp_segment": segment,
                    "is_target": is_target,
                    "channel_name": channel,
                    "payer_group": payer_group,
                    "strength_mg": strength,
                    "trx": trx,
                    "nrx": nrx,
                    "units": units,
                    "net_product_sales": round(net_sales, 2),
                    "conversion_rate": round(conversion_rate, 3),
                    "payer_reimb_rate": round(payer_reimb_rate, 3),
                }
            )

    df = pd.DataFrame(rows)
    out_path = RAW_DIR / f"iqvia_rx_{month}.csv"
    df.to_csv(out_path, index=False)
    print(f"Saved {out_path} with shape {df.shape}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--month", help="Single month YYYY-MM, e.g. 2024-03")
    parser.add_argument("--start", help="Start month YYYY-MM")
    parser.add_argument("--end", help="End month YYYY-MM (inclusive)")
    parser.add_argument("--hcps_per_state", type=int, default=30)
    args = parser.parse_args()

    if args.month:
        generate_iqvia_rx(args.month, args.hcps_per_state)
    elif args.start and args.end:
        start_y, start_m = map(int, args.start.split("-"))
        end_y, end_m = map(int, args.end.split("-"))

        y, m = start_y, start_m
        while (y < end_y) or (y == end_y and m <= end_m):
            month_str = f"{y:04d}-{m:02d}"
            generate_iqvia_rx(month_str, args.hcps_per_state)
            m += 1
            if m > 12:
                m = 1
                y += 1
    else:
        raise SystemExit("请提供 --month 或 (--start 和 --end)")

if __name__ == "__main__":
    main()
