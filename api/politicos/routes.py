from flask import request, jsonify
from sqlalchemy import text
from api.common.db import engine
from . import politicos_bp

@politicos_bp.route("/", methods=["GET"])
def list_politicos():
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    offset = (page - 1) * per_page

    nome = request.args.get("nome", "").strip().lower()
    sql = """
        SELECT DISTINCT txNomeParlamentar AS nome, cpf, sgPartido AS partido, sgUF AS uf
        FROM gastos_parlamentares
        WHERE (:nome = '' OR LOWER(txNomeParlamentar) LIKE :nome_pattern)
        ORDER BY txNomeParlamentar
        LIMIT :limit OFFSET :offset
    """
    params = {
        "nome": nome,
        "nome_pattern": f"{nome}%",
        "limit": per_page,
        "offset": offset,
    }

    with engine.connect() as conn:
        result = conn.execute(text(sql), params).mappings().all()
        items = [dict(row) for row in result]


    return jsonify({
        "page": page,
        "per_page": per_page,
        "results": items
    })
