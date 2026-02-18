from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import OrdemProd
from src.service import OrdemProd_service

OrdemProd_routes = Blueprint('OrdemProd_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@OrdemProd_routes.route('/pcp/api/OrdemProd_porSku', methods=['GET'])
@token_required
def get_OrdemProd_porSku():
    codSku = request.args.get('codSku','1')

    dados = OrdemProd.OrdemProd('1',str(codSku)).get_OrdemProdSku()
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


@OrdemProd_routes.route('/pcp/api/FilaOps_requisicao', methods=['GET'])
@token_required
def get_FilaOps_requisicao():
    codEmpresa = str(request.args.get('codEmpresa','1'))


    dados = OrdemProd_service.OrdemProd_service(codEmpresa).ordemProd_requisicao_gerada()
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


@OrdemProd_routes.route('/pcp/api/DetalhamentoRequisicao', methods=['GET'])
@token_required
def get_DetalhamentoRequisicao():
    codEmpresa = str(request.args.get('codEmpresa','1'))
    codRequisicao = str(request.args.get('codRequisicao','1'))


    dados = OrdemProd_service.OrdemProd_service(codEmpresa).detalhar_requisicao(codRequisicao)
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


@OrdemProd_routes.route('/pcp/api/UsuarioHabilitadoAviamento', methods=['GET'])
@token_required
def get_UsuarioHabilitadoAviamento():
    codEmpresa = str(request.args.get('codEmpresa','1'))


    dados = OrdemProd_service.OrdemProd_service(codEmpresa).buscar_usuarios_habilitados()
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