# fetch_data.py
from clickhouse_driver import Client
from datetime import timedelta
import math
import config


# =========================
# 工具函数
# =========================
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


def in_confidence_range(value, field):
    """
    判断数值是否在 config.SENSOR_CONFIDENCE_RANGE 中
    """
    if value is None:
        return False

    rule = config.SENSOR_CONFIDENCE_RANGE.get(field)
    if not rule:
        # 未配置置信区间 → 视为可信
        return True

    return rule["min"] <= value <= rule["max"]


def fetch_last_valid_value(client, device_id, field, end_time):
    """
    在 DATA_MAX_LOOKBACK_MIN 时间内
    向前查找最近一个【置信区间内】的值
    """
    start_time = end_time - timedelta(minutes=config.DATA_MAX_LOOKBACK_MIN)

    rows = client.execute(
        f"""
        SELECT {field}, create_time_min
        FROM iot_db.snow_device_data
        WHERE device_id = %(device_id)s
          AND create_time_min < %(end)s
          AND create_time_min >= %(start)s
        ORDER BY create_time_min DESC
        """,
        {
            "device_id": device_id,
            "end": end_time,
            "start": start_time,
        },
    )

    for v, t in rows:
        v = _to_float(v)
        if in_confidence_range(v, field):
            return v, t

    return None, None


# =========================
# 主函数
# =========================
def fetch_sensor_data():
    client = Client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB,
    )

    latest_rows = client.execute(
        """
        SELECT
            device_id,
            any(device_name) AS device_name,
            max(create_time_min) AS latest_time
        FROM iot_db.snow_device_data
        GROUP BY device_id
        """
    )

    results = {}

    for device_id, device_name, anchor_time in latest_rows:
        t24 = anchor_time - timedelta(hours=24)
        t72 = anchor_time - timedelta(hours=72)

        missing_reasons = {}

        # =========================
        # 1️⃣ 锚点原始数据
        # =========================
        row = client.execute(
            """
            SELECT snow_depth, wind_speed, atmospheric_temperature
            FROM iot_db.snow_device_data
            WHERE device_id = %(id)s AND create_time_min = %(t)s
            LIMIT 1
            """,
            {"id": device_id, "t": anchor_time},
        )

        snow_depth_mm, wind_speed, temp_now = map(
            _to_float, row[0] if row else (None, None, None)
        )

        # =========================
        # 2️⃣ snow_depth（mm → m，带回溯）
        # =========================
        if not in_confidence_range(snow_depth_mm, "snow_depth"):
            fallback, ft = fetch_last_valid_value(
                client, device_id, "snow_depth", anchor_time
            )
            if fallback is None:
                snow_depth_m = None
                missing_reasons["snow_depth"] = "out_of_range_and_no_fallback"
            else:
                snow_depth_m = fallback / 1000.0
                missing_reasons["snow_depth"] = f"fallback_used@{ft}"
        else:
            snow_depth_m = (
                snow_depth_mm / 1000.0 if snow_depth_mm is not None else None
            )
            if snow_depth_mm is None:
                missing_reasons["snow_depth"] = "raw_missing"

        # =========================
        # 3️⃣ 24h / 72h 雪深（回溯）
        # =========================
        def snow_depth_at(t):
            v, _ = fetch_last_valid_value(client, device_id, "snow_depth", t)
            return v / 1000.0 if v is not None else None

        snow_24 = snow_depth_at(t24)
        snow_72 = snow_depth_at(t72)

        if snow_24 is None:
            missing_reasons["snow_depth_24h"] = "no_valid_value"
        if snow_72 is None:
            missing_reasons["snow_depth_72h"] = "no_valid_value"

        # =========================
        # 4️⃣ 派生量
        # =========================
        delta_snow_24h = (
            snow_depth_m - snow_24
            if snow_depth_m is not None and snow_24 is not None
            else None
        )

        snowfall_24h = delta_snow_24h
        snowfall_72h = (
            snow_depth_m - snow_72
            if snow_depth_m is not None and snow_72 is not None
            else None
        )

        # =========================
        # 5️⃣ 24h 累计降水（mm → m）
        # =========================
        rain_row = client.execute(
            """
            SELECT sum(toFloat64OrZero(precipitation))
            FROM iot_db.snow_device_data
            WHERE device_id = %(id)s
              AND create_time_min > %(t24)s
              AND create_time_min <= %(t0)s
            """,
            {"id": device_id, "t24": t24, "t0": anchor_time},
        )

        rainfall_24h_mm = _to_float(rain_row[0][0])
        rainfall_24h = (
            rainfall_24h_mm / 1000.0 if rainfall_24h_mm is not None else None
        )

        if rainfall_24h_mm is None:
            missing_reasons["precipitation_24h"] = "raw_missing"

        # =========================
        # 6️⃣ 24h 温度
        # =========================
        temp_row = client.execute(
            """
            SELECT avg(toFloat64OrNull(atmospheric_temperature)),
                   min(toFloat64OrNull(atmospheric_temperature))
            FROM iot_db.snow_device_data
            WHERE device_id = %(id)s
              AND create_time_min > %(t24)s
              AND create_time_min <= %(t0)s
            """,
            {"id": device_id, "t24": t24, "t0": anchor_time},
        )

        temp_avg_24h, temp_min_24h = map(_to_float, temp_row[0])

        temp_delta_24h = (
            temp_now - temp_min_24h
            if temp_now is not None and temp_min_24h is not None
            else None
        )

        if temp_avg_24h is None:
            missing_reasons["temp_avg_24h"] = "raw_missing"

        # =========================
        # 7️⃣ 输出
        # =========================
        results[device_id] = {
            "device_id": device_id,
            "device_name": device_name,
            "anchor_time": anchor_time.strftime("%Y-%m-%d %H:%M:%S"),

            "snow_depth": snow_depth_m,
            "delta_snow_24h": delta_snow_24h,
            "snowfall_24h": snowfall_24h,
            "snowfall_72h": snowfall_72h,

            "temp_avg_24h": temp_avg_24h,
            "temp_delta_24h": temp_delta_24h,

            "rainfall_24h": rainfall_24h,
            "wind_speed": wind_speed,

            "missing_reasons": missing_reasons,
            # ✅ 旧字段（兼容 test / api）
            "missing_fields": sorted(missing_reasons.keys()),
        }

    return results


# 向后兼容
fetch_data = fetch_sensor_data