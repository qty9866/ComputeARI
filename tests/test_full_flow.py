# tests/test_full_flow.py
import json
from datetime import datetime

from fetch_data import fetch_data
from compute_ari import compute_all_ari
from write_result import write_ari_results
from app import create_app


def test_full_flow():
    print("=" * 80)
    print("[TEST] FULL FLOW TEST START")
    print("=" * 80)

    # ===== 1. 取数（以设备最新时间为锚点）=====
    sensor_data = fetch_data()
    print(f"[TEST] fetched devices: {list(sensor_data.keys())}")

    if not sensor_data:
        print("[TEST] ❌ no data fetched")
        return

    # ===== 2. 打印原始取数结果（你明确要求的）=====
    for did, data in sensor_data.items():
        print(f"\n[RAW DATA] device={did}")
        for k, v in data.items():
            print(f"  {k}: {v}")

    # ===== 3. ARI 计算 =====
    ari_results = compute_all_ari(sensor_data)
    print("\n[TEST] compute_all_ari result:")
    print(json.dumps(ari_results, indent=2, ensure_ascii=False, default=str))

    # ===== 4. 写入数据库 =====
    # ⚠️ 注意：这里的 ari_time 只是“记录时间”，不是计算锚点
    ari_time = datetime.now().replace(second=0, microsecond=0)

    try:
        write_ari_results(ari_results, ari_time)
        print("[TEST] ✅ write_ari_results done")
    except Exception as e:
        print(f"[TEST] ❌ ClickHouse insert failed: {e}")

    # ===== 5. API 测试 =====
    app = create_app()
    client = app.test_client()

    resp = client.get("/api/ari/latest")
    print("\n[TEST] API status code:", resp.status_code)
    print("[TEST] API response:")
    print(resp.get_data(as_text=True))

    print("=" * 80)
    print("[TEST] FULL FLOW TEST END")
    print("=" * 80)


if __name__ == "__main__":
    test_full_flow()