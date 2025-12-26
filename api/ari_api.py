# api/ari_api.py
from flask import Blueprint, jsonify, request
from fetch_data import fetch_sensor_data
from compute_ari import compute_all_ari

ari_bp = Blueprint("ari", __name__)  # ❗不带 /api


@ari_bp.route("/ari", methods=["GET"])
def get_ari():
    print("[API] /ari called")

    device_id = request.args.get("device_id")
    print(f"[API] device_id = {device_id}")

    # 1️⃣ 取数
    try:
        sensor_data = fetch_sensor_data()
        print(f"[API] fetched {len(sensor_data)} devices")
    except Exception as e:
        print("[API] fetch_sensor_data ERROR:", repr(e))
        return jsonify({
            "success": False,
            "msg": "fetch_sensor_data failed",
            "error": str(e)
        }), 500

    if not sensor_data:
        return jsonify({
            "success": False,
            "msg": "No sensor data",
            "data": {}
        }), 200

    # 2️⃣ 计算 ARI
    try:
        ari_results = compute_all_ari(sensor_data)
        print("[API] compute_ari finished")
    except Exception as e:
        print("[API] compute_ari ERROR:", repr(e))
        return jsonify({
            "success": False,
            "msg": "compute_ari failed",
            "error": str(e)
        }), 500

    # 3️⃣ 设备过滤
    if device_id:
        data = ari_results.get(device_id)
        if data is None:
            return jsonify({
                "success": False,
                "msg": f"Device {device_id} not found",
                "data": {}
            }), 200
    else:
        data = ari_results

    return jsonify({
        "success": True,
        "data": data
    }), 200