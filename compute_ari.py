# compute_ari.py
from typing import Dict, Any
import math


# =========================
# 工具函数
# =========================
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


def clamp_ari(v):
    if v is None:
        return None
    return max(0.0, v)


def ari_level_from_value(ari):
    """
    二维 ARI 分级（单模型）
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


# =========================
# 单设备 ARI 计算
# =========================
def compute_ari_for_device(data: Dict[str, Any]) -> Dict[str, Any]:
    missing = set(data.get("missing_fields", []))

    # -------------------------
    # 基础变量（fetch_data 已统一单位）
    # -------------------------
    snow_depth = safe_float(data.get("snow_depth"))          # m
    snowfall_24h = safe_float(data.get("snowfall_24h"))      # m
    snowfall_72h = safe_float(data.get("snowfall_72h"))      # m
    delta_snow_24h = safe_float(data.get("delta_snow_24h"))  # m

    temp_avg_24h = safe_float(data.get("temp_avg_24h"))      # ℃
    rainfall_24h = safe_float(data.get("rainfall_24h"))      # mm
    wind_speed = safe_float(data.get("wind_speed"))          # m/s

    # ======================================================
    # 模型 1：降雪诱发雪崩 I
    # ======================================================
    ari_1 = None
    ari_1_level = "无"

    if not {"snow_depth", "snowfall_24h"} & missing:
        if snow_depth is not None and snowfall_24h is not None:
            ari_1 = ((snow_depth / 0.6) + (snowfall_24h / 0.015)) / 2
            ari_1 = clamp_ari(ari_1)
            ari_1_level = ari_level_from_value(ari_1)

    # ======================================================
    # 模型 2：降雪诱发雪崩 II
    # ======================================================
    ari_2 = None
    ari_2_level = "无"

    if not {"snow_depth", "delta_snow_24h"} & missing:
        if snow_depth is not None and delta_snow_24h is not None:
            ari_2 = ((snow_depth / 0.6) + (delta_snow_24h / 0.2)) / 2
            ari_2 = clamp_ari(ari_2)
            ari_2_level = ari_level_from_value(ari_2)

    # ======================================================
    # 模型 3：增温融雪诱发雪崩 I
    # ======================================================
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

    # ======================================================
    # 模型 4：增温融雪诱发雪崩 II（降雨）
    # ======================================================
    ari_4 = "无"
    if snow_depth is not None and rainfall_24h is not None:
        if snow_depth > 0.3:
            if rainfall_24h > 5:
                ari_4 = "I"
            elif rainfall_24h > 0:
                ari_4 = "II"

    # ======================================================
    # 模型 5：风吹雪诱发雪崩
    # ======================================================
    ari_5 = "无"
    if wind_speed is not None:
        if wind_speed >= 12:
            ari_5 = "I"
        elif wind_speed >= 10:
            ari_5 = "II"
        elif wind_speed >= 8:
            ari_5 = "III"
        elif wind_speed >= 5:
            ari_5 = "IV"

    # ======================================================
    # 一维雪崩监测阈值模型（表 2）
    # ======================================================
    one_d_level = "无"

    if snow_depth is not None and snow_depth > 0.6:

        # 红色
        if (
            (snowfall_72h is not None and snowfall_72h >= 0.5) or
            (delta_snow_24h is not None and delta_snow_24h <= -0.25 and temp_avg_24h is not None and temp_avg_24h > 0) or
            (wind_speed is not None and wind_speed >= 10)
        ):
            one_d_level = "红"

        # 橙色
        elif (
            (snowfall_72h is not None and 0.3 <= snowfall_72h < 0.5) or
            (delta_snow_24h is not None and -0.25 < delta_snow_24h <= -0.2 and temp_avg_24h is not None and temp_avg_24h > 0) or
            (wind_speed is not None and 8 <= wind_speed < 10)
        ):
            one_d_level = "橙"

        # 黄色
        elif (
            (snowfall_72h is not None and 0.2 <= snowfall_72h < 0.3) or
            (delta_snow_24h is not None and -0.2 < delta_snow_24h <= -0.15 and temp_avg_24h is not None and temp_avg_24h > 0) or
            (wind_speed is not None and 6 <= wind_speed < 8)
        ):
            one_d_level = "黄"

        # 蓝色
        elif (
            (snowfall_72h is not None and 0.1 <= snowfall_72h < 0.2) or
            (delta_snow_24h is not None and -0.15 < delta_snow_24h <= -0.05 and temp_avg_24h is not None and temp_avg_24h > 0) or
            (wind_speed is not None and 5 <= wind_speed < 6)
        ):
            one_d_level = "蓝"

    # =============================
    # 输出
    # =============================
    return {
        "ari_1": ari_1,
        "ari_1_level": ari_1_level,

        "ari_2": ari_2,
        "ari_2_level": ari_2_level,

        "ari_3": ari_3,
        "ari_4": ari_4,
        "ari_5": ari_5,

        "one_d_warning_level": one_d_level,
        "missing_fields": list(missing),
    }


def compute_all_ari(fetch_result: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    result = {}
    for device_id, data in fetch_result.items():
        result[device_id] = compute_ari_for_device(data)
    return result