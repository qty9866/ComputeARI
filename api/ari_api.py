from flask import Blueprint, jsonify, request
from fetch_data import fetch_data
from compute_ari import compute_all_ari

ari_bp = Blueprint('ari', __name__, url_prefix='/api')


@ari_bp.route('/ari', methods=['GET'])
def get_ari():
    """
    获取最新 ARI 结果
    可选参数：
        device_id: 指定设备 id
    """

    device_id = request.args.get('device_id')

    # 1️⃣ 取数（以设备最新时间为锚点）
    sensor_data = fetch_data()
    if not sensor_data:
        return jsonify({
            "success": False,
            "msg": "No sensor data",
            "data": {}
        }), 200

    # 2️⃣ 计算 ARI
    ari_results = compute_all_ari(sensor_data)

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