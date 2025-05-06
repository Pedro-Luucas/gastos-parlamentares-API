import json
from flask import Response, request, jsonify
from sqlalchemy import text

from . import gastos_bp
from decimal import Decimal
from api.common.db import engine

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

    # SQL principal com campos adicionais no formato da API oficial
    sql = text(f"""
        SELECT
            numano AS ano,
            nummes AS mes,
            txtdescricao AS tipoDespesa,
            idedocumento AS codDocumento,
            indtipodocumento AS tipodocumento,
            indtipodocumento AS codTipoDocumento,
            datemissao AS dataDocumento,
            txtnumero AS numDocumento,
            CAST(vlrDocumento AS NUMERIC) AS valorDocumento,
            urlDocumento AS urlDocumento,
            txtfornecedor AS nomeFornecedor,
            txtcnpjcpf AS cnpjCpfFornecedor,
            CAST(vlrliquido AS NUMERIC) AS valorLiquido,
            COALESCE(CAST(vlrglosa AS NUMERIC), 0) AS valorGlosa,
            COALESCE(numressarcimento, '') AS numRessarcimento,
            COALESCE(numlote, '0') AS codLote,
            COALESCE(numparcela, '0') AS parcela
        FROM gastos_parlamentares
        WHERE {where_sql}
        ORDER BY datemissao DESC
        LIMIT :limit OFFSET :offset
    """)

    # SQL para contagem total
    count_sql = text(f"""
        SELECT COUNT(*) FROM gastos_parlamentares WHERE {where_sql}
    """)

    params_with_pagination = dict(params)
    params_with_pagination.update({
        "limit": page_size,
        "offset": offset
    })

    with engine.connect() as conn:
        # Contagem
        total = conn.execute(count_sql, params).scalar()

        # Dados paginados
        result = conn.execute(sql, params_with_pagination)
        items = [dict(row._mapping) for row in result]


        tipo_documento_map = {
    "0": "Nota Fiscal",
    "1": "Recibo",
    "2": "Despesa no Exterior",
    "3": "Despesa do Parlasul",
    "4": "DANFE/DACTE"
    }

    for item in items:
        if "tipoDocumento" in item:
            item["tipoDocumento"] = tipo_documento_map.get(item["tipoDocumento"], "Outro")

    items = [{k: convert_decimal(v) for k, v in item.items()} for item in items]

    response = Response(
        json.dumps({
            "page": page,
            "page_size": page_size,
            "total": total,
            "results": items
        }, ensure_ascii=False),
        content_type="application/json; charset=utf-8"
    )
    response.headers["X-Total-Count"] = str(total)

    return response
