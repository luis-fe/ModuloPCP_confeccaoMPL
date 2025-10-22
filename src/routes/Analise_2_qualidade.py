from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Analise_2_qualidade

Analise_2_qualidade_routes = Blueprint('Analise_2_qualidade_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Analise_2_qualidade_routes.route('/api/Dashboard2Qualidade', methods=['GET'])
@token_required
def get_Dashboar2Qualidade():
    codEmpresa = request.args.get('codEmpresa', '1')
    data_inicio = request.args.get('data_inicio', '-')
    data_fim = request.args.get('data_fim', '-')

    dados = Analise_2_qualidade.Analise_2_qualidade(codEmpresa,data_inicio,data_fim).dashboard_TOTAL_tags_2_qualidade_periodo()
    #controle.salvarStatus(rotina, ip, datainicio)

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



@Analise_2_qualidade_routes.route('/api/MotivosAgrupado', methods=['GET'])
@token_required
def get_MotivosAgrupado():
    codEmpresa = request.args.get('codEmpresa', '1')
    data_inicio = request.args.get('data_inicio', '-')
    data_fim = request.args.get('data_fim', '-')

    dados = Analise_2_qualidade.Analise_2_qualidade(codEmpresa,data_inicio,data_fim).motivos_agrupo_periodo()
    #controle.salvarStatus(rotina, ip, datainicio)

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


@Analise_2_qualidade_routes.route('/api/defeitos_faccionista_agrupo_periodo', methods=['GET'])
@token_required
def get_defeitos_faccionista_agrupo_periodo():
    codEmpresa = request.args.get('codEmpresa', '1')
    data_inicio = request.args.get('data_inicio', '-')
    data_fim = request.args.get('data_fim', '-')

    dados = Analise_2_qualidade.Analise_2_qualidade(codEmpresa,data_inicio,data_fim).defeitos_faccionista_agrupo_periodo()
    #controle.salvarStatus(rotina, ip, datainicio)

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

