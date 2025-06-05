from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Tendencia_Plano

Tendencia_Plano_routes = Blueprint('Tendencia_Plano_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@Tendencia_Plano_routes.route('/pcp/api/consultaPlanejamentoABC_plano', methods=['GET'])
@token_required
def get_consultaPlanejamentoABC_plano():

    codPlano = request.args.get('codPlano','-')
    dados = Tendencia_Plano.Tendencia_Plano('1',codPlano).consultaPlanejamentoABC()
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




@Tendencia_Plano_routes.route('/pcp/api/ABCReferencia', methods=['POST'])
@token_required
def post_ABCReferencia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')


    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaAbc()

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


@Tendencia_Plano_routes.route('/pcp/api/tendenciaSku', methods=['POST'])
@token_required
def post_tendenciaSku():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    congelar = data.get('congelar',False)

    if congelar == False:
        dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaVendas()
    else:
        print(data)
        dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaCongeladSku()

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

@Tendencia_Plano_routes.route('/pcp/api/obtendoUltimaTendencia_porPlano', methods=['GET'])
@token_required
def obtendoUltimaTendencia_porPlano():

    codPlano = request.args.get('codPlano','-')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Tendencia_Plano.Tendencia_Plano(codEmpresa,codPlano).obtendoUltimaTendencia()
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

@Tendencia_Plano_routes.route('/pcp/api/simulacaoProgramacao', methods=['POST'])
@token_required
def post_simulacaoProgramacao():
    '''Api que simulaca a "tendendcia de vendas baseado" no modelo montando pelo usuario '''
    data = request.get_json()
    print(data)
    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    nomeSimulacao = data.get('nomeSimulacao')

    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq,nomeSimulacao).simulacaoPeloNome()
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


@Tendencia_Plano_routes.route('/pcp/api/simulacaoDetalhadaPorSku', methods=['POST'])
@token_required
def post_simulacaoDetalhadaPorSku():
    data = request.get_json()
    print(data)

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    nomeSimulacao = data.get('nomeSimulacao','')
    codSku = data.get('codSku')

    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq,nomeSimulacao, codSku).detalhaCalculoPrev()
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