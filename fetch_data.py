# fetch_data.py
from clickhouse_driver import Client
from datetime import datetime, timedelta
import math
import config

# =========================
# 基础工具
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


def get_ch_client():
    return Client(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        user=config.CLICKHOUSE_USER,
        password=config.CLICKHOUSE_PASSWORD,
        database=config.CLICKHOUSE_DB,
    )


def in_confidence_range(value, field):
    if value is None:
        return False
    rule = config.SENSOR_CONFIDENCE_RANGE.get(field)
    if not rule:
        return True
    return rule["min"] <= value <= rule["max"]


# =========================
# 时间策略
# =========================

def get_calc_anchor_time():
    return datetime.now() - timedelta(minutes=config.DATA_DELAY_GUARD_MIN)


# =========================
# 单字段可信回溯
# =========================

def fetch_last_valid_value(client, device_id, field, end_time):
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
# 传感器数据（供 ARI 计算）
# =========================

def fetch_sensor_data():
    client = get_ch_client()
    anchor_time = get_calc_anchor_time()

    results = {}

    for device_id in config.DEVICE_IDS:

        missing_fields = []

        # ---------- 雪深 ----------
        snow_depth_mm, _ = fetch_last_valid_value(
            client, device_id, "snow_depth", anchor_time
        )
        snow_depth = snow_depth_mm / 1000.0 if snow_depth_mm is not None else None
        if snow_depth is None:
            missing_fields.append("snow_depth")

        # ---------- 历史雪深 ----------
        def snow_depth_at(hours):
            t = anchor_time - timedelta(hours=hours)
            v, _ = fetch_last_valid_value(client, device_id, "snow_depth", t)
            return v / 1000.0 if v is not None else None

        snow_24 = snow_depth_at(24)
        snow_72 = snow_depth_at(72)

        snowfall_24h = (
            snow_depth - snow_24
            if snow_depth is not None and snow_24 is not None
            else None
        )

        snowfall_72h = (
            snow_depth - snow_72
            if snow_depth is not None and snow_72 is not None
            else None
        )

        delta_snow_24h = (
            snow_24 - snow_depth
            if snow_depth is not None and snow_24 is not None
            else None
        )

        if snowfall_24h is None:
            missing_fields.append("snowfall_24h")
        if snowfall_72h is None:
            missing_fields.append("snowfall_72h")
        if delta_snow_24h is None:
            missing_fields.append("delta_snow_24h")

        # ---------- 风速 ----------
        wind_speed, _ = fetch_last_valid_value(
            client, device_id, "wind_speed", anchor_time
        )
        if wind_speed is None:
            missing_fields.append("wind_speed")

        # ---------- 24h 平均温度 ----------
        t24 = anchor_time - timedelta(hours=24)
        rows = client.execute(
            """
            SELECT atmospheric_temperature
            FROM iot_db.snow_device_data
            WHERE device_id = %(device_id)s
              AND create_time_min < %(end)s
              AND create_time_min >= %(start)s
            """,
            {
                "device_id": device_id,
                "end": anchor_time,
                "start": t24,
            },
        )

        temps = [
            _to_float(r[0])
            for r in rows
            if in_confidence_range(_to_float(r[0]), "atmospheric_temperature")
        ]
        temp_avg_24h = sum(temps) / len(temps) if temps else None
        if temp_avg_24h is None:
            missing_fields.append("temp_avg_24h")

        # ---------- 24h 累计降雨 ----------
        rows = client.execute(
            """
            SELECT rainfall
            FROM iot_db.snow_device_data
            WHERE device_id = %(device_id)s
              AND create_time_min < %(end)s
              AND create_time_min >= %(start)s
            """,
            {
                "device_id": device_id,
                "end": anchor_time,
                "start": t24,
            },
        )

        rainfall_vals = [
            _to_float(r[0])
            for r in rows
            if in_confidence_range(_to_float(r[0]), "rainfall")
        ]
        rainfall_24h = sum(rainfall_vals) if rainfall_vals else None
        if rainfall_24h is None:
            missing_fields.append("rainfall_24h")

        results[device_id] = {
            "device_id": device_id,
            "device_name": device_id,
            "anchor_time": anchor_time.strftime("%Y-%m-%d %H:%M:%S"),

            "snow_depth": snow_depth,
            "snowfall_24h": snowfall_24h,
            "snowfall_72h": snowfall_72h,
            "delta_snow_24h": delta_snow_24h,

            "wind_speed": wind_speed,
            "temp_avg_24h": temp_avg_24h,
            "rainfall_24h": rainfall_24h,

            "missing_fields": list(set(missing_fields)),
        }

    return results


# =========================
# ARI 历史回溯（完全保持原逻辑）
# =========================

def fetch_ari_last_valid_n(device_id: str, n: int = 7):
    client = get_ch_client()

    result = {f"ari_{i}": [] for i in range(1, 6)}

    for i in range(1, 6):
        field = f"ari_{i}"
        rows = client.execute(
            f"""
            SELECT {field}
            FROM iot_db.snow_device_ari
            WHERE device_id = %(device_id)s
              AND {field} IS NOT NULL
            ORDER BY ari_time DESC
            LIMIT %(limit)s
            """,
            {
                "device_id": device_id,
                "limit": n,
            },
        )
        result[field] = [str(r[0]) for r in reversed(rows)]

    return result
