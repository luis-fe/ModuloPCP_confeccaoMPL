from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import DashboardTV
import pandas as pd

dashboard_fat_routes = Blueprint('dashboard_fat_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@dashboard_fat_routes.route('/pcp/api/get_metas_cadastradas_ano_empresa', methods=['GET'])
@token_required
def get_metas_cadastradas_ano_empresa():
    codEmpresa = request.args.get('Empresa','1')
    ano =  request.args.get('ano','1')

    codEmpresa = str(codEmpresa)
    codEmpresa = codEmpresa.upper()


    if codEmpresa == 'FILIAL':
        codEmpresa = '4'

    if codEmpresa == 'MATRIZ':
        codEmpresa = '1'

    dados = DashboardTV.DashboardTV(codEmpresa, ano).get_metas_cadastradas_ano_empresa()

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


@dashboard_fat_routes.route('/pcp/api/post_atualizarMetaMesesAno', methods=['POST'])
@token_required
def post_atualizarMetaMesesAno():

    data = request.get_json()
    codEmpresa = data.get('Empresa')
    ano = data.get('ano')
    meses = data.get('meses')
    metas = data.get('metas')

    codEmpresa = str(codEmpresa)
    codEmpresa = codEmpresa.upper()


    if codEmpresa == 'FILIAL':
        codEmpresa = '4'

    if codEmpresa == 'MATRIZ':
        codEmpresa = '1'

    dados = DashboardTV.DashboardTV(codEmpresa,ano,meses,metas).post_metas_empresa_ano()

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




@dashboard_fat_routes.route('/pcp/api/devolver_nome_usuario', methods=['GET'])
@token_required
def get_usuarios():
    matricula = request.args.get('matricula')

    dados = DashboardTV.DashboardTV('','','','',str(matricula)).devolver_nome_usuario()

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


@dashboard_fat_routes.route('/pcp/api/autentificarUsuarioSenhaMetas', methods=['GET'])
@token_required
def get_autentificarUsuarioSenhaMetas():
    matricula = request.args.get('matricula')
    senha = request.args.get('senha')

    dados = DashboardTV.DashboardTV('','','','',matricula, '',senha).usuario_autentificar()

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