# ==============================
# ClickHouse 配置
# ==============================
CLICKHOUSE_HOST = "192.168.144.63"
CLICKHOUSE_HTTP_PORT = 41730   # HTTP 端口
CLICKHOUSE_TCP_PORT = 30201    # TCP 端口（备用）
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "Flzx3qc@"
CLICKHOUSE_DB = "iot_db"

# 默认使用 TCP
CLICKHOUSE_PORT = CLICKHOUSE_TCP_PORT

# ==============================
# ARI 服务管理的设备白名单
# ==============================
DEVICE_IDS = [
    "04672adb0c3a",
    "0a5c2c269035",
    "182e883da115",
    "33df2866852b",
    "39d5b2111266",
    "rn5610f4dm3u",
    "wk330ae903c5",
]


# ==============================
# 时间策略（分钟）
# ==============================
ARI_INTERVAL_MIN = 30        # ARI 计算周期（前端 30 分钟一次）
DATA_DELAY_GUARD_MIN = 2     # 写入延迟保护（分钟）

# 最大可回溯可信时间（分钟）
DATA_MAX_LOOKBACK_MIN = 2880


# ==============================
# 超出范围 → 视为漂移
# ==============================
SENSOR_CONFIDENCE_RANGE = {

    # 雪深（mm）
    "snow_depth": {
        "min": 0,
        "max": 10000,          # 10m上限
        "unit": "mm",
    },

    # 1分钟降水量 / 累计降水（mm）
    "precipitation": {
        "min": 0,
        "max": 50,             # 1分钟 50mm
        "unit": "mm",
    },

    # 风速（m/s）
    "wind_speed": {
        "min": 0,
        "max": 60,             # 台风级上限
        "unit": "m/s",
    },

    # 大气温度（℃）
    "atmospheric_temperature": {
        "min": -50,
        "max": 50,
        "unit": "℃",
    },

    # 大气湿度（%）
    "atmospheric_humidity": {
        "min": 0,
        "max": 100,
        "unit": "%",
    },

    # 大气压力（kPa）
    "atmospheric_pressure": {
        "min": 60,
        "max": 110,
        "unit": "kPa",
    },
}


# ============================== 
# ARI 缺失 / 漂移兜底策略
# ==============================
ARI_FALLBACK = {
    # 超过 MAX_BACKTRACK_MIN 仍无可信数据
    "ari_value": 0.0,
    "ari_level": "I",        # 最低风险等级
}