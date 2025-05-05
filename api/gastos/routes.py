import json
from flask import Response, request, jsonify
from sqlalchemy import text
from api.common.db import engine
from . import gastos_bp
from decimal import Decimal

def convert_decimal(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    return obj

@gastos_bp.route("/politico/<idecadastro>", methods=["GET"])
def gastos_por_politico(idecadastro):
    ano_from  = request.args.get("year_from")
    ano_to    = request.args.get("year_to")
    categoria = request.args.get("categoria")
    page      = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 50))
    offset    = (page - 1) * page_size

    idecadastro_lower = idecadastro.strip().lower()
    where_clauses = ["LOWER(idecadastro) = :idecadastro"]
    params = {"idecadastro": idecadastro_lower}

    if ano_from:
        where_clauses.append("numano >= :numano_from")
        params["numano_from"] = ano_from
    if ano_to:
        where_clauses.append("numano <= :numano_to")
        params["numano_to"] = ano_to
    if categoria:
        where_clauses.append("LOWER(txtdescricao) LIKE :categoria")
        params["categoria"] = f"%{categoria.lower()}%"

    where_sql = " AND ".join(where_clauses)

    sql = text(f"""
        SELECT
            datEmissao    AS data,
            txtdescricao  AS categoria,
            txtfornecedor AS fornecedor,
            CAST(vlrDocumento AS NUMERIC) AS valor,
            urlDocumento  AS url,
            ano
        FROM gastos_parlamentares
        WHERE {where_sql}
        ORDER BY datEmissao DESC
        LIMIT :limit OFFSET :offset
    """)

    params["limit"] = page_size
    params["offset"] = offset

    with engine.connect() as conn:
        result = conn.execute(sql, params)
        items = [dict(row._mapping) for row in result]

    # Convert Decimal para float antes de fazer dump
    items = [{k: convert_decimal(v) for k, v in item.items()} for item in items]

    return Response(
        json.dumps({
            "page": page,
            "page_size": page_size,
            "results": items
        }, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )



@gastos_bp.route("/aggregate", methods=["GET"])
def gastos_aggregate():
    """
    Agrega gastos por uma chave (politico, partido, uf, categoria).
    Query params:
      - group_by: politico|partido|uf|categoria
      - year_from, year_to
      - page, page_size
    """
    group_by = request.args.get("group_by", "politico")
    ano_from = request.args.get("year_from")
    ano_to   = request.args.get("year_to")
    page      = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 50))
    offset    = (page - 1) * page_size

    key_map = {
        "politico": 'txNomeParlamentar',
        "partido":  'sgPartido',
        "uf":       'sgUF',
        "categoria": 'txtDescricao'
    }

    if group_by not in key_map:
        return jsonify({"error": "group_by inválido"}), 400

    key_col = key_map[group_by]
    params = {}
    filters = []

    if ano_from:
        filters.append('"ano" >= :ano_from')
        params["ano_from"] = ano_from
    if ano_to:
        filters.append('"ano" <= :ano_to')
        params["ano_to"] = ano_to

    where_sql = ("WHERE " + " AND ".join(filters)) if filters else ""

    sql = text(f"""
        SELECT {key_col} AS key,
               SUM(CAST(vlrDocumento AS NUMERIC)) AS total
        FROM gastos_parlamentares
        {where_sql}
        GROUP BY {key_col}
        ORDER BY total DESC
        LIMIT :limit OFFSET :offset
    """)

    params["limit"] = page_size
    params["offset"] = offset

    with engine.connect() as conn:
        results = conn.execute(sql, params)
        items = [dict(row._mapping) for row in results]

    return jsonify({
        "page": page,
        "page_size": page_size,
        "results": items
    })