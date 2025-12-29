# app.py
from flask import Flask
from api.ari_api import ari_bp

def create_app():
    """
    创建 Flask 应用
    """
    app = Flask(__name__)

    # 注册 ARI Blueprint
    app.register_blueprint(ari_bp, url_prefix='/api')

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(host='0.0.0.0', port=8000, debug=True)