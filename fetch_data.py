# fetch_data.py
from clickhouse_driver import Client
from datetime import timedelta
import math
import config


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


def fetch_sensor_data():
    """
    以【设备最新数据时间】为锚点，而不是当前时间

    返回结构：
    {
        device_id: {
            device_id,
            device_name,
            anchor_time,
            snow_depth,
            delta_snow_24h,
            snowfall_24h,
            snowfall_72h,
            temp_avg_24h,
            temp_delta_24h,
            rainfall_24h,
            wind_speed,
            missing_fields
        }
    }
    """

    client = Client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB,
    )

    # 1️⃣ 每个设备的最新分钟时间（锚点）
    latest_sql = """
    SELECT
        device_id,
        any(device_name) AS device_name,
        max(create_time_min) AS latest_time
    FROM iot_db.snow_device_data
    GROUP BY device_id
    """
    latest_rows = client.execute(latest_sql)

    results = {}

    for device_id, device_name, anchor_time in latest_rows:
        t24 = anchor_time - timedelta(hours=24)
        t72 = anchor_time - timedelta(hours=72)

        missing = []

        # =========================
        # 2️⃣ 锚点时刻数据
        # =========================
        anchor_row = client.execute(
            """
            SELECT
                snow_depth,
                wind_speed,
                atmospheric_temperature
            FROM iot_db.snow_device_data
            WHERE device_id = %(device_id)s
              AND create_time_min = %(t)s
            LIMIT 1
            """,
            {"device_id": device_id, "t": anchor_time},
        )

        if anchor_row:
            snow_depth, wind_speed, temp_now = map(_to_float, anchor_row[0])
        else:
            snow_depth = wind_speed = temp_now = None

        if snow_depth is None:
            missing.append("snow_depth")
        if wind_speed is None:
            missing.append("wind_speed")
        if temp_now is None:
            missing.append("atmospheric_temperature(now)")

        # =========================
        # 3️⃣ 24h / 72h 前雪深
        # =========================
        def fetch_last_snow_depth(t):
            row = client.execute(
                """
                SELECT snow_depth
                FROM iot_db.snow_device_data
                WHERE device_id = %(device_id)s
                  AND create_time_min <= %(t)s
                ORDER BY create_time_min DESC
                LIMIT 1
                """,
                {"device_id": device_id, "t": t},
            )
            return _to_float(row[0][0]) if row else None

        snow_24 = fetch_last_snow_depth(t24)
        snow_72 = fetch_last_snow_depth(t72)

        if snow_24 is None:
            missing.append("snow_depth_24h")
        if snow_72 is None:
            missing.append("snow_depth_72h")

        # =========================
        # 4️⃣ 24h 累计降水（mm → m）
        # =========================
        rain_row = client.execute(
            """
            SELECT sum(toFloat64OrZero(precipitation))
            FROM iot_db.snow_device_data
            WHERE device_id = %(device_id)s
              AND create_time_min > %(t24)s
              AND create_time_min <= %(t0)s
            """,
            {"device_id": device_id, "t24": t24, "t0": anchor_time},
        )
        rainfall_24h_mm = _to_float(rain_row[0][0])
        if rainfall_24h_mm is None:
            missing.append("precipitation_24h")

        rainfall_24h = rainfall_24h_mm / 1000 if rainfall_24h_mm is not None else None

        # =========================
        # 5️⃣ 24h 温度统计
        # =========================
        temp_row = client.execute(
            """
            SELECT
                avg(toFloat64OrNull(atmospheric_temperature)),
                min(toFloat64OrNull(atmospheric_temperature))
            FROM iot_db.snow_device_data
            WHERE device_id = %(device_id)s
              AND create_time_min > %(t24)s
              AND create_time_min <= %(t0)s
            """,
            {"device_id": device_id, "t24": t24, "t0": anchor_time},
        )

        temp_avg_24h, temp_min_24h = map(_to_float, temp_row[0])

        if temp_avg_24h is None:
            missing.append("temp_avg_24h")
        if temp_min_24h is None:
            missing.append("temp_min_24h")

        temp_delta_24h = (
            temp_now - temp_min_24h
            if temp_now is not None and temp_min_24h is not None
            else None
        )

        # =========================
        # 6️⃣ 派生量
        # =========================
        delta_snow_24h = (
            snow_depth - snow_24
            if snow_depth is not None and snow_24 is not None
            else None
        )

        snowfall_24h = delta_snow_24h
        snowfall_72h = (
            snow_depth - snow_72
            if snow_depth is not None and snow_72 is not None
            else None
        )

        results[device_id] = {
            "device_id": device_id,
            "device_name": device_name,
            "anchor_time": anchor_time.strftime("%Y-%m-%d %H:%M:%S"),

            "snow_depth": snow_depth,
            "delta_snow_24h": delta_snow_24h,
            "snowfall_24h": snowfall_24h,
            "snowfall_72h": snowfall_72h,

            "temp_avg_24h": temp_avg_24h,
            "temp_delta_24h": temp_delta_24h,

            "rainfall_24h": rainfall_24h,
            "wind_speed": wind_speed,

            "missing_fields": sorted(set(missing)),
        }

    return results


# ✅ 向后兼容（关键）
# 你之前 test / api / app 用的就是这个名字
fetch_data = fetch_sensor_data