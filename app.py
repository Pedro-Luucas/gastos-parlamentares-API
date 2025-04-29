from flask import Flask
from api.politicos import politicos_bp
from api.gastos import gastos_bp   

def create_app():
    app = Flask(__name__)
    app.config.from_pyfile("config.py")

    app.register_blueprint(politicos_bp, url_prefix="/api/v1/politicos")
    app.register_blueprint(gastos_bp,     url_prefix="/api/v1/gastos")  

    return app

if __name__ == "__main__":
    app = create_app()
    app.run(debug=True)
