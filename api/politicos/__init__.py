from flask import Blueprint

politicos_bp = Blueprint("politicos", __name__)

from . import routes
