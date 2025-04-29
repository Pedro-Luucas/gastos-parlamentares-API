from flask import Blueprint

gastos_bp = Blueprint("gastos", __name__)

from . import routes
