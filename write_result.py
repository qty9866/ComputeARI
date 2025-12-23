# write_result.py

import clickhouse_connect
from datetime import datetime
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_HTTP_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
)


TABLE = "snow_device_ari"

COLUMNS = [
    "device_id",
    "device_name",
    "ari_1",
    "ari_2",
    "ari_3",
    "ari_4",
    "ari_5",
    "ari_level",
    "threshold_level",
    "threshold_reason",
    "calc_window_24h",
    "calc_window_72h",
    "data_quality_flag",
    "ari_time",
]


def _fmt(v):
    """
    所有 ARI 数值统一：
    - None -> None
    - float -> 保留 2 位小数的字符串
    - 其他 -> str
    """
    if v is None:
        return None
    if isinstance(v, float):
        return f"{v:.2f}"
    return str(v)


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_HTTP_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


def write_ari_results(results_dict: dict, ari_time: datetime):
    if not results_dict:
        print("[ARI] empty result, skip insert")
        return

    client = get_client()
    rows = []

    for device_id, res in results_dict.items():
        rows.append([
            str(device_id),
            str(res.get("device_name", "")),

            _fmt(res.get("ari_1")),
            _fmt(res.get("ari_2")),
            _fmt(res.get("ari_3")),
            _fmt(res.get("ari_4")),
            _fmt(res.get("ari_5")),

            _fmt(res.get("ari_level")),
            _fmt(res.get("threshold_level")),
            _fmt(res.get("threshold_reason")),

            _fmt(res.get("calc_window_24h", "Y")),
            _fmt(res.get("calc_window_72h", "Y")),
            _fmt(res.get("data_quality_flag", "normal")),

            ari_time,
        ])

    try:
        client.insert(
            TABLE,
            data=rows,
            column_names=COLUMNS,
        )
        print(f"[ARI] ✅ inserted {len(rows)} rows into {TABLE}")

    except Exception as e:
        print("[ARI] ❌ ClickHouse insert failed")
        print("Exception type:", type(e))
        print("Exception repr:", repr(e))
        raise