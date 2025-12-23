# config.py

# ClickHouse 配置
CLICKHOUSE_HOST = "192.168.144.63"
CLICKHOUSE_HTTP_PORT = 41730   # HTTP 端口
CLICKHOUSE_TCP_PORT = 30201    # TCP 端口（备用）
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "Flzx3qc@"
CLICKHOUSE_DB = "iot_db"
# config.py
CLICKHOUSE_PORT = CLICKHOUSE_TCP_PORT

# ARI 服务管理的设备白名单
DEVICE_IDS = [
    "04672adb0c3a",
    "0a5c2c269035",
    "182e883da115",
    "33df2866852b",
    "39d5b2111266",
    "rn5610f4dm3u",
    "wk330ae903c5",
]

# 时间策略（分钟）
ARI_INTERVAL_MIN = 30  # 标准计算步长 
DATA_DELAY_GUARD_MIN = 5 # 延迟写入缓冲时长