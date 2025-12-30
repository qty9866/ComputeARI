# api/ari_api.py
from flask import Blueprint, jsonify, request

from fetch_data import (
    fetch_sensor_data,
    fetch_ari_last_valid_n
)
from compute_ari import compute_all_ari

ari_bp = Blueprint("ari", __name__)


@ari_bp.route("/ari", methods=["GET"])
def get_ari():
    """
    GET /api/ari
    GET /api/ari?device_id=xxx

    返回：
    - 不带参数：所有设备当前 ARI
    - 带 device_id：该设备 最近7条有效ARI（字符串）
    """

    device_id = request.args.get("device_id")

    # =========================
    # 1️⃣ 计算当前 ARI（你原有逻辑）
    # =========================
    sensor_data = fetch_sensor_data()
    ari_now = compute_all_ari(sensor_data)

    # =========================
    # 2️⃣ 单设备：历史 + 当前
    # =========================
    if device_id:
        if device_id not in ari_now:
            return jsonify({
                "success": False,
                "msg": f"device_id {device_id} not found"
            }), 200

        # 最近 7 条【有值】ARI（字符串）
        history = fetch_ari_last_valid_n(
            device_id=device_id,
            n=6
        )

        # 当前值（统一转字符串，空的给 ""）
        current = ari_now.get(device_id, {})

        for k in ["ari_1", "ari_2", "ari_3", "ari_4", "ari_5"]:
            v = current.get(k)
            history[k].append("" if v is None else str(v))

        return jsonify({
            "success": True,
            "device_id": device_id,
            "data": history
        }), 200

    # =========================
    # 3️⃣ 不带参数：当前全量
    # =========================
    return jsonify({
        "success": True,
        "data": ari_now
    }), 200