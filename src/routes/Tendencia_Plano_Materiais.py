from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Tendencia_Plano_Materiais, Produtos

Tendencia_Plano_Materiais_routes = Blueprint('Tendencia_Plano_Materiais_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Tendencia_Plano_Materiais_routes.route('/pcp/api/AnaliseMateriaisPelaTendencia', methods=['POST'])
@token_required
def post_AnaliseMateriaisPelaTendencia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codEmpresa = data.get('codEmpresa','1')


    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq).estruturaItens('nao')
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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/AnaliseMateriaisPelaSimulacao', methods=['POST'])
@token_required
def post_AnaliseMateriaisPelaSimulacao():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codEmpresa = data.get('codEmpresa','1')
    nomeSimulacao = data.get('nomeSimulacao')


    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq,'',nomeSimulacao).estruturaItens('nao','nao','sim')
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



@Tendencia_Plano_Materiais_routes.route('/pcp/api/CalculoPcs_baseaado_MP', methods=['POST'])
@token_required
def post_CalculoPcs_baseaado_MP():
    data = request.get_json()


    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codEmpresa = data.get('codEmpresa','1')
    arrayCategoriaMP = data.get('arrayCategoriaMP','')
    print(data)

    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq).calculoIdealPcs_para_materiaPrima('nao',arrayCategoriaMP)
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


@Tendencia_Plano_Materiais_routes.route('/pcp/api/naturezaEstoqueComponentes', methods=['GET'])
@token_required
def ger_naturezaEstoqueComponentes():

    codEmpresa = request.args.get('codEmpresa','1')

    dados = Produtos.Produtos(codEmpresa).estMateriaPrima_nomes()
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


@Tendencia_Plano_Materiais_routes.route('/pcp/api/DetalhaNecessidade', methods=['POST'])
@token_required
def post_DetalhaNecessidade():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codComponente = data.get('codComponente')
    nomeSimulacao = data.get("nomeSimulacao",'nao')


    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais('1',codPlano, consideraPedBloq,'',
                                                                str(codComponente)).detalhaNecessidade(nomeSimulacao)
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


@Tendencia_Plano_Materiais_routes.route('/pcp/api/categroriaMP', methods=['GET'])
@token_required
def get_categroriaMP():

    codEmpresa = request.args.get('codEmpresa','1')

    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa).obter_categoriasMP()
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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/detalharSku_x_AnaliseEmpenho', methods=['POST'])
@token_required
def post_detalharSku_x_AnaliseEmpenhoe():
    data = request.get_json()

    codEmpresa = data.get('codEmpresa','1')
    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codReduzido = data.get('codReduzido')
    nomeSimulacao = data.get("nomeSimulacao",'nao')
    arrayCategoriaMP = data.get('arrayCategoriaMP','')



    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa,codPlano, consideraPedBloq,'','',nomeSimulacao,
                                                                str(codReduzido)).detalharSku_x_AnaliseEmpenho(nomeSimulacao, arrayCategoriaMP)
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