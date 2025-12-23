# time_utils.py

from datetime import datetime, timedelta

def floor_to_interval(dt: datetime, interval_min: int = 30) -> datetime:
    """
    将时间向下取整到指定分钟间隔
    例如 interval_min=30, 08:37 -> 08:30
    """
    return dt.replace(minute=(dt.minute // interval_min) * interval_min, second=0, microsecond=0)


def get_time_ranges(ari_time: datetime, data_delay_guard_min: int = 5):
    """
    根据 ARI 计算时间和延迟缓冲，返回各窗口时间段
    返回字典：
    {
        "short_window": (start, end),
        "window_24h": (start, end),
        "window_72h": (start, end)
    }
    """
    end_time = ari_time - timedelta(minutes=data_delay_guard_min)
    return {
        "short_window": (end_time - timedelta(minutes=30), end_time),
        "window_24h": (end_time - timedelta(hours=24), end_time),
        "window_72h": (end_time - timedelta(hours=72), end_time),
    }