from clickhouse_driver import Client
from datetime import timedelta
import config
import math

DEVICE_ID = "04672adb0c3a"  # 大岩洞基站


def _to_float(v):
    try:
        if v is None:
            return None
        v = float(v)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def main():
    client = Client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB,
    )

    print("=" * 90)
    print(f"[DEBUG] DEVICE = {DEVICE_ID}")
    print("=" * 90)

    # ------------------------------------------------------------
    # 1️⃣ anchor_time（最新分钟）
    # ------------------------------------------------------------
    latest_sql = """
    SELECT
        device_id,
        any(device_name),
        max(create_time_min)
    FROM iot_db.snow_device_data
    WHERE device_id = %(device_id)s
    GROUP BY device_id
    """
    row = client.execute(latest_sql, {"device_id": DEVICE_ID})[0]
    device_id, device_name, anchor_time = row

    print("[1] ANCHOR TIME")
    print(f"  device_name = {device_name}")
    print(f"  anchor_time = {anchor_time}")

    t24 = anchor_time - timedelta(hours=24)
    t72 = anchor_time - timedelta(hours=72)

    print(f"  t24 = {t24}")
    print(f"  t72 = {t72}")

    # ------------------------------------------------------------
    # 2️⃣ anchor 时刻原始值
    # ------------------------------------------------------------
    anchor_sql = """
    SELECT
        snow_depth,
        wind_speed,
        atmospheric_temperature
    FROM iot_db.snow_device_data
    WHERE device_id = %(device_id)s
      AND create_time_min = %(t)s
    LIMIT 1
    """
    anchor_row = client.execute(
        anchor_sql, {"device_id": DEVICE_ID, "t": anchor_time}
    )

    print("\n[2] ANCHOR RAW ROW")
    if anchor_row:
        sd, ws, temp = map(_to_float, anchor_row[0])
        print(f"  snow_depth(now) = {sd}")
        print(f"  wind_speed(now) = {ws}")
        print(f"  temperature(now) = {temp}")
    else:
        print("  ❌ NO ROW FOUND")

    # ------------------------------------------------------------
    # 3️⃣ 24h 前 最近一次 snow_depth
    # ------------------------------------------------------------
    def fetch_last_snow_depth(t, label):
        sql = """
        SELECT
            create_time_min,
            snow_depth
        FROM iot_db.snow_device_data
        WHERE device_id = %(device_id)s
          AND create_time_min <= %(t)s
        ORDER BY create_time_min DESC
        LIMIT 1
        """
        rows = client.execute(sql, {"device_id": DEVICE_ID, "t": t})
        print(f"\n[3] LAST SNOW DEPTH <= {label}")
        if rows:
            ct, val = rows[0]
            print(f"  time = {ct}")
            print(f"  snow_depth = {val}")
            return _to_float(val)
        else:
            print("  ❌ NO DATA")
            return None

    snow_24 = fetch_last_snow_depth(t24, "t24")
    snow_72 = fetch_last_snow_depth(t72, "t72")

    # ------------------------------------------------------------
    # 4️⃣ 派生量计算（完全和 fetch_data 一致）
    # ------------------------------------------------------------
    print("\n[4] DERIVED CALCULATION")

    snow_now = sd if anchor_row else None

    delta_snow_24h = (
        snow_now - snow_24
        if snow_now is not None and snow_24 is not None
        else None
    )

    snowfall_24h = delta_snow_24h

    snowfall_72h = (
        snow_now - snow_72
        if snow_now is not None and snow_72 is not None
        else None
    )

    print(f"  snow_depth(now)     = {snow_now}")
    print(f"  snow_depth(24h)     = {snow_24}")
    print(f"  snow_depth(72h)     = {snow_72}")
    print("-" * 60)
    print(f"  delta_snow_24h      = {delta_snow_24h}")
    print(f"  snowfall_24h        = {snowfall_24h}")
    print(f"  snowfall_72h        = {snowfall_72h}")

    print("\n[END DEBUG]")
    print("=" * 90)


if __name__ == "__main__":
    main()