# fetch_sensor_realtime.py
from clickhouse_driver import Client
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB
)
from datetime import datetime
import logging

# 配置日志（可选）
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 创建 ClickHouse 客户端（使用 TCP 协议）
client = Client(
    host=CLICKHOUSE_HOST,
    port=CLICKHOUSE_PORT,
    user=CLICKHOUSE_USER,
    password=CLICKHOUSE_PASSWORD,
    database=CLICKHOUSE_DB,
    send_receive_timeout=10
)

# 需要查询的字段和前端展示名称映射
FIELD_MAPPING = {
    "temperature": "atmospheric_temperature",           # 温度
    "humidity": "atmospheric_humidity",                  # 湿度
    "pressure": "atmospheric_pressure",                  # 气压
    "precipitation": "precipitation",                    # 降雪量（称重式）
    "snow_depth": "snow_depth",                          # 雪深
    "wind_direction": "wind_direction",                  # 风向
    "wind_speed": "wind_speed",                          # 风速
    "x_wind_speed": "x_wind_speed",                      # X轴风速
    "y_wind_speed": "y_wind_speed",                      # Y轴风速
    "z_wind_speed": "z_wind_speed",                      # Z轴风速
    "rainfall": "rainfall"                               # 分钟降水量（翻斗式）
}

def fetch_realtime_sensor_data(device_id: str) -> dict:
    """
    获取指定设备每个传感器参数的最新非空值
    :param device_id: 设备ID
    :return: dict，键为前端字段名，值为字符串或 null
    """
    result = {
        "temperature": None,
        "humidity": None,
        "pressure": None,
        "precipitation": None,
        "snow_depth": None,
        "wind_direction": None,
        "wind_speed": None,
        "x_wind_speed": None,
        "y_wind_speed": None,
        "z_wind_speed": None,
        "rainfall": None,
        "update_time": None  # 可选：记录最新更新时间（取所有字段中最新的create_time）
    }

    latest_time = None

    try:
        for display_key, column_name in FIELD_MAPPING.items():
            query = f"""
                SELECT {column_name}, create_time
                FROM {CLICKHOUSE_DB}.snow_device_data
                WHERE device_id = %(device_id)s
                  AND {column_name} IS NOT NULL
                  AND {column_name} != ''
                ORDER BY create_time DESC
                LIMIT 1
            """

            rows = client.execute(query, {'device_id': device_id})

            if rows:
                value, create_time = rows[0]
                result[display_key] = str(value) if value is not None else None
                # 更新整体最新时间
                if latest_time is None or create_time > latest_time:
                    latest_time = create_time
            else:
                result[display_key] = None

        if latest_time:
            result["update_time"] = latest_time.strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"Successfully fetched realtime data for device_id: {device_id}")
        return result

    except Exception as e:
        logger.error(f"Error fetching data for device_id {device_id}: {str(e)}")
        # 出错时返回全 None
        return {k: None for k in result.keys()}