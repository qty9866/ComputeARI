# api/sensor_api.py
from flask import Blueprint, request, jsonify
from config import DEVICE_IDS
from fetch_sensor_realtime import fetch_realtime_sensor_data

sensor_api = Blueprint("sensor_api", __name__)


@sensor_api.route("/sensor", methods=["GET"])
def get_sensor_data():
    device_id = request.args.get("device_id")

    if not device_id:
        return jsonify({
            "success": False,
            "msg": "device_id required"
        }), 400

    if device_id not in DEVICE_IDS:
        return jsonify({
            "success": False,
            "msg": "device_id not allowed"
        }), 403

    data = fetch_realtime_sensor_data(device_id)

    return jsonify({
        "success": True,
        "device_id": device_id,
        "data": {
            "temperature": data["temperature"],
            "humidity": data["humidity"],
            "pressure": data["pressure"],
            "precipitation": data["precipitation"],
            "snow_depth": data["snow_depth"],
            "wind_direction": data["wind_direction"],
            "wind_speed": data["wind_speed"],
            "x_wind_speed": data["x_wind_speed"],
            "y_wind_speed": data["y_wind_speed"],
            "z_wind_speed": data["z_wind_speed"],
            "rainfall": data["rainfall"],
            "update_time": data["update_time"]  # 新增：最近更新时间
        }
    })