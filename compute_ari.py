# compute_ari.py
from typing import Dict, Any
import math


def safe_float(x):
    try:
        if x is None:
            return None
        v = float(x)
        if math.isnan(v):
            return None
        return v
    except Exception:
        return None


def clamp_ari(value):
    """
    ARI < 0 一律置 0
    """
    if value is None:
        return None
    return max(0.0, value)


def ari_level_from_value(ari: float):
    """
    二维 ARI 等级
    """
    if ari is None:
        return "无"
    if ari >= 1.0:
        return "I"
    if 0.9 <= ari < 1.0:
        return "II"
    if 0.7 <= ari < 0.9:
        return "III"
    if 0.5 <= ari < 0.7:
        return "IV"
    return "无"


def compute_ari_for_device(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    单设备 ARI 计算
    data 来自 fetch_data，已经是“最新时间锚点”
    """

    missing = set(data.get("missing_fields", []))

    # 基础变量（单位统一）
    snow_depth = safe_float(data.get("snow_depth"))           # m
    snowfall_24h = safe_float(data.get("snowfall_24h"))       # m
    delta_snow_24h = safe_float(data.get("delta_snow_24h"))   # m
    temp_avg_24h = safe_float(data.get("temp_avg_24h"))       # ℃
    rainfall_24h = safe_float(data.get("rainfall_24h"))       # mm
    wind_speed = safe_float(data.get("wind_speed"))           # m/s

    # =============================
    # 模型 1：降雪诱发 I
    # =============================
    ari_1 = None
    if not {"snow_depth", "snowfall_24h"} & missing:
        if snow_depth is not None and snowfall_24h is not None:
            ari_1 = ((snow_depth / 0.6) + (snowfall_24h / 0.015)) / 2
            ari_1 = clamp_ari(ari_1)

    # =============================
    # 模型 2：降雪诱发 II
    # =============================
    ari_2 = None
    if not {"snow_depth", "delta_snow_24h"} & missing:
        if snow_depth is not None and delta_snow_24h is not None:
            ari_2 = ((snow_depth / 0.6) + (delta_snow_24h / 0.2)) / 2
            ari_2 = clamp_ari(ari_2)

    # =============================
    # 模型 3：增温融雪 I（等级型）
    # =============================
    ari_3 = "无"
    if temp_avg_24h is not None and delta_snow_24h is not None:
        if temp_avg_24h > 0:
            if delta_snow_24h > 0.25:
                ari_3 = "I"
            elif delta_snow_24h > 0.2:
                ari_3 = "II"
            elif delta_snow_24h > 0.15:
                ari_3 = "III"
            elif delta_snow_24h > 0.1:
                ari_3 = "IV"

    # =============================
    # 模型 4：增温融雪 II（降雨）
    # =============================
    ari_4 = "无"
    if snow_depth is not None and rainfall_24h is not None:
        if snow_depth > 0.3:
            if rainfall_24h > 5:
                ari_4 = "I"
            elif rainfall_24h > 0:
                ari_4 = "II"

    # =============================
    # 模型 5：风吹雪
    # =============================
    ari_5 = 0
    if wind_speed is not None:
        if wind_speed >= 12:
            ari_5 = 4
        elif wind_speed >= 10:
            ari_5 = 3
        elif wind_speed >= 8:
            ari_5 = 2
        elif wind_speed >= 5:
            ari_5 = 1
        else:
            ari_5 = 0

    # =============================
    # 综合等级（取最危险）
    # =============================
    numeric_levels = []

    for a in [ari_1, ari_2]:
        if a is not None:
            numeric_levels.append(a)

    final_level = "无"
    if numeric_levels:
        final_level = ari_level_from_value(max(numeric_levels))

    return {
        "ari_1": ari_1,
        "ari_2": ari_2,
        "ari_3": ari_3,
        "ari_4": ari_4,
        "ari_5": ari_5,
        "ari_level": final_level,
        "missing_fields": list(missing)
    }


def compute_all_ari(fetch_result: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    对所有设备计算 ARI
    """
    result = {}
    for device_id, data in fetch_result.items():
        result[device_id] = compute_ari_for_device(data)
    return result