from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Pedidos

pedidosPlano_routes = Blueprint('pedidosPlano_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@pedidosPlano_routes.route('/pcp/api/TipoNotasCsw', methods=['GET'])
@token_required
def get_TipoNotasCsw():
    empresa = request.args.get('empresa','1')


    dados = Pedidos.Pedidos(empresa).obtert_tipoNotas()

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


@pedidosPlano_routes.route('/pcp/api/VincularNotasPlano', methods=['PUT'])
@token_required
def put_VincularNotasPlano():

    data = request.get_json()

    empresa = request.args.get('empresa','1')
    codigoPlano = data.get('codigoPlano')
    arrayTipoNotas = data.get('arrayTipoNotas', '-')


    dados = Pedidos.Pedidos(empresa, codigoPlano).vincularNotasAoPlano(arrayTipoNotas)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@pedidosPlano_routes.route('/pcp/api/VendasPorPlano', methods=['POST'])
@token_required
def post_VendasPorPlano():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedidosBloqueado = data.get('consideraPedidosBloqueado','nao')


    dados = Pedidos.Pedidos('1',codPlano,consideraPedidosBloqueado).vendasGeraisPorPlano()
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


@pedidosPlano_routes.route('/pcp/api/VendasPlanoSKU', methods=['POST'])
@token_required
def post_VendasPlanoSKU():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedidosBloqueado = data.get('consideraPedidosBloqueado','nao')


    dados = Pedidos.Pedidos('1',codPlano, consideraPedidosBloqueado).vendasPorSku()
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


@pedidosPlano_routes.route('/pcp/api/DetalhaPedidosSKU', methods=['GET'])
@token_required
def get_DetalhaPedidosSKU():

    codPlano = request.args.get('codPlano', '-')
    consideraPedidosBloqueado = request.args.get('consideraPedidosBloqueado', 'nao')
    codReduzido = request.args.get('codReduzido', '-')


    dados = Pedidos.Pedidos('1',str(codPlano),consideraPedidosBloqueado,str(codReduzido)).detalhaPedidosSku()
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



