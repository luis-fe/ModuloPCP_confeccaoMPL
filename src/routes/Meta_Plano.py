from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Meta_Plano, Plano, Plano_Lote

metaPlano_routes = Blueprint('metaPlano_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@metaPlano_routes.route('/pcp/api/MetaGeralPlano', methods=['GET'])
@token_required
def get_MetaGeralPlano():
    codPlano = request.args.get('codPlano','-')
    codEmpresa = request.args.get('1','-')

    dados = Meta_Plano.Meta_Plano(codEmpresa, codPlano).consultaMetaGeral()
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



@metaPlano_routes.route('/pcp/api/inserirOuAtualizarMetaPlano', methods=['POST'])
@token_required
def post_inserirOuAtualizarMetaPlano():
    data = request.get_json()

    codPlano = data.get('codPlano')
    marca = data.get('marca', '-')
    metaFinanceira = data.get('metaFinanceira', '-')
    metaPecas = data.get('metaPecas', '-')
    codEmpresa = request.args.get('1','-')


    dados = Meta_Plano.Meta_Plano(codEmpresa, codPlano,marca, metaFinanceira, metaPecas).inserirOuAtualizarMetasGeraisPlano()

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


@metaPlano_routes.route('/pcp/api/consultarMetaCategoriaPlano', methods=['GET'])
@token_required
def get_consultarMetaCategoriaPlano():
    codPlano = request.args.get('codPlano', '-')
    marca = request.args.get('marca', '-')
    codEmpresa = request.args.get('1','-')


    dados = Meta_Plano.Meta_Plano(codEmpresa, codPlano,marca).consultarMetaCategoriaPlano()
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


@metaPlano_routes.route('/pcp/api/ConsultaColecaoVinculados', methods=['GET'])
@token_required
def get_ConsultaColecaoVinculados():
    codPlano = request.args.get('codPlano')
    codEmpresa = request.args.get('codEmpresa','1')

    dados = Plano.Plano(codPlano,"","","","","","",codEmpresa).obterColecoesporPlano()
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


@metaPlano_routes.route('/pcp/api/ConsultaLotesVinculados', methods=['GET'])
@token_required
def GET_ConsultaLotesVinculados():
    planoParametro = request.args.get('plano', '-')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Plano.Plano(planoParametro,"","","","","","",codEmpresa).obterLotesporPlano()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)



@metaPlano_routes.route('/pcp/api/ConsultaTipoNotasVinculados', methods=['GET'])
@token_required
def GET_ConsultaTipoNotasVinculados():
    planoParametro = request.args.get('plano', '-')
    codEmpresa = request.args.get('codEmpresa','1')

    dados = Plano.Plano(planoParametro,"","","","","","",codEmpresa).ConsultarTipoNotasVinculados()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@metaPlano_routes.route('/pcp/api/lotes_csw', methods=['GET'])
@token_required
def get_lotes_csw():
    empresa = request.args.get('empresa','1')

    dados = Plano_Lote.Plano_Lote(empresa).loteCsw()

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
