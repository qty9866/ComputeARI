# main.py

import time
from datetime import datetime
from fetch_data import fetch_sensor_data
from compute_ari import compute_all_ari
from write_result import write_ari_results
from config import ARI_INTERVAL_MIN, DATA_DELAY_GUARD_MIN

def run_once():
    """
    单次抓取数据、计算 ARI 并写入数据库
    """
    ari_time = datetime.now()
    print(f"[ARI] start calc at {ari_time}")

    # 获取传感器数据
    sensor_data = fetch_sensor_data(ari_time)
    if not sensor_data:
        print("[ARI] no data fetched, skip")
        return

    # 计算 ARI
    ari_results = compute_all_ari(sensor_data)

    # 写入 ClickHouse
    write_ari_results(ari_results, ari_time)

    print(f"[ARI] finished calc at {datetime.now()}")


def scheduler_loop():
    """
    定时任务循环，每 ARI_INTERVAL_MIN 分钟执行一次
    """
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[ARI] Exception occurred: {e}")
        # 等待下一个时间点
        time.sleep(ARI_INTERVAL_MIN * 60)


if __name__ == "__main__":
    print("[ARI] Scheduler started")
    scheduler_loop()