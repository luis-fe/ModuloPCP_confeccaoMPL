from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Meta_Plano, Plano, Metas_Ano

metaAno_routes = Blueprint('metaAno_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@metaAno_routes.route('/pcp/api/inserirOuAtualizarMetaAno', methods=['POST'])
@token_required
def post_inserirOuAtualizarMetaAno():
    data = request.get_json()

    codUsuario = data.get('codUsuario')
    nomeUsuario = data.get('nomeUsuario', '-')
    codEmpresa = data.get('codEmpresa', '-')
    ano = data.get('ano', '-')
    arrayMes = request.args.get('arrayMes','-')
    arrayNovaMeta = request.args.get('arrayNovaMeta','-')


    dados =''
    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    del dados
    return jsonify(OP_data)
