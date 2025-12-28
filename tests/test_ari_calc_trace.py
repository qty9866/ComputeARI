# tests/test_ari_calc_trace.py

from fetch_data import fetch_sensor_data
from compute_ari import compute_ari_for_device
from datetime import datetime

DEBUG_DEVICE = "wk330ae903c5"


def print_sep(title):
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def main():
    print_sep("STEP 1: FETCH SENSOR DATA (raw)")

    data = fetch_sensor_data()

    if DEBUG_DEVICE not in data:
        print(f"[ERROR] device {DEBUG_DEVICE} not found in fetch result")
        return

    d = data[DEBUG_DEVICE]

    # -----------------------------
    # 原始取数结果
    # -----------------------------
    print(f"device_id        = {d['device_id']}")
    print(f"device_name      = {d['device_name']}")
    print(f"anchor_time      = {d['anchor_time']}")
    print(f"missing_fields   = {d['missing_fields']}")

    print_sep("STEP 2: RAW VALUES (AS USED IN COMPUTE_ARI)")

    snow_depth = d["snow_depth"]
    snow_24 = snow_depth - d["delta_snow_24h"] if d["delta_snow_24h"] is not None else None

    print(f"snow_depth(now)        = {snow_depth}")
    print(f"snow_depth(24h ago)    = {snow_24}")
    print(f"delta_snow_24h         = {d['delta_snow_24h']}")
    print(f"snowfall_24h           = {d['snowfall_24h']}")
    print(f"snowfall_72h           = {d['snowfall_72h']}")
    print(f"rainfall_24h (m)       = {d['rainfall_24h']}")
    print(f"temp_avg_24h (℃)       = {d['temp_avg_24h']}")
    print(f"wind_speed (m/s)       = {d['wind_speed']}")

    print_sep("STEP 3: MANUAL ARI-1 CALCULATION TRACE")

    if snow_depth is not None and d["snowfall_24h"] is not None:
        part1 = snow_depth / 0.6
        part2 = d["snowfall_24h"] / 0.015
        ari_1 = (part1 + part2) / 2

        print("ARI-1 FORMULA:")
        print("  ((snow_depth / 0.6) + (snowfall_24h / 0.015)) / 2")
        print("")
        print(f"  snow_depth / 0.6      = {snow_depth} / 0.6 = {part1}")
        print(f"  snowfall_24h / 0.015  = {d['snowfall_24h']} / 0.015 = {part2}")
        print(f"  ARI-1 (raw)           = ({part1} + {part2}) / 2 = {ari_1}")
    else:
        print("[SKIP] ARI-1 missing data")

    print_sep("STEP 4: COMPUTE_ARI OUTPUT")

    ari_result = compute_ari_for_device(d)
    for k, v in ari_result.items():
        print(f"{k:15s} = {v}")


if __name__ == "__main__":
    main()