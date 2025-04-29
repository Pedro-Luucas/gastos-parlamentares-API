from flask import request, jsonify
from sqlalchemy import text
from api.common.db import engine
from . import gastos_bp

@gastos_bp.route("/politico/<cpf>", methods=["GET"])
def gastos_por_politico(cpf):
    ano_from  = request.args.get("year_from")
    ano_to    = request.args.get("year_to")
    categoria = request.args.get("categoria")
    cpf_lower = cpf.strip().lower()

    # Construir cláusulas de WHERE
    where_clauses = ["LOWER(cpf) = :cpf"]    # <-- força LOWER aqui
    params = {"cpf": cpf_lower}

    if ano_from:
        where_clauses.append("ano >= :ano_from")
        params["ano_from"] = ano_from
    if ano_to:
        where_clauses.append("ano <= :ano_to")
        params["ano_to"] = ano_to
    if categoria:
        where_clauses.append("LOWER(txtdescricao) LIKE :categoria")
        params["categoria"] = f"%{categoria.lower()}%"

    where_sql = " AND ".join(where_clauses)

    sql = text(f"""
        SELECT
            datEmissao    AS data,
            txtdescricao  AS categoria,
            txtfornecedor AS fornecedor,
            CAST(vlrdocumento AS NUMERIC) AS valor,
            urldocumento  AS url,
            ano
        FROM gastos_parlamentares
        WHERE {where_sql}
        ORDER BY datEmissao DESC
        LIMIT 1000
    """)

    with engine.connect() as conn:
        result = conn.execute(sql, params)
        # Converte cada RowMapping em dict
        items = [ dict(row._mapping) for row in result ]

    return jsonify(items)


@gastos_bp.route("/aggregate", methods=["GET"])
def gastos_aggregate():
    """
    Agrega gastos por uma chave (politico, partido, uf, categoria).
    Query params:
      - group_by: politico|partido|uf|categoria
      - year_from, year_to
    """
    group_by = request.args.get("group_by", "politico")
    ano_from = request.args.get("year_from")
    ano_to   = request.args.get("year_to")

    key_map = {
        "politico": 'txNomeParlamentar',
        "partido":   'sgPartido',
        "uf":        'sgUF',
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
        LIMIT 50
    """)

    with engine.connect() as conn:
        results = conn.execute(sql, params).mappings().all()

    return jsonify(results)
