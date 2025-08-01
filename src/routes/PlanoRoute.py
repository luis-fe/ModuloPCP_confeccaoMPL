from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Plano

plano_routes = Blueprint('plano_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@plano_routes.route('/pcp/api/Plano', methods=['GET'])
@token_required
def get_Plano():

    codEmpresa = request.args.get('codEmpresa','1')

    dados = Plano.Plano('','','','','','','',codEmpresa).obterPlanos()
    return jsonify(dados)

@plano_routes.route('/pcp/api/PlanoPorPlano', methods=['GET'])
@token_required
def get_PlanoPorPlano():
    codigoPlano = request.args.get('codigoPlano')

    dados = Plano.Plano(codigoPlano).obterPlanosPlano()
    return jsonify(dados)


@plano_routes.route('/pcp/api/NovoPlano', methods=['POST'])
@token_required
def pOST_novoPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    descricaoPlano = data.get('descricaoPlano', '-')
    iniVendas = data.get('iniVendas', '-')
    fimVendas = data.get('fimVendas', '-')
    iniFat = data.get('iniFat', '-')
    fimFat = data.get('fimFat', '-')
    usuarioGerador = data.get('usuarioGerador', '-')


    dados = Plano.Plano(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat, usuarioGerador).inserirNovoPlano()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@plano_routes.route('/pcp/api/AlterPlano', methods=['PUT'])
@token_required
def PUT_AlterPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    descricaoPlano = data.get('descricaoPlano', '-')
    iniVendas = data.get('iniVendas', '-')
    fimVendas = data.get('fimVendas', '-')
    iniFat = data.get('iniFat', '-')
    fimFat = data.get('fimFat', '-')


    dados = Plano.Plano(codigoPlano, descricaoPlano, iniVendas, fimVendas, iniFat, fimFat).alterarPlano()
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)


@plano_routes.route('/pcp/api/DesvincularNotasPlano', methods=['DELETE'])
@token_required
def Delete_DesvincularNotasPlano():

    data = request.get_json()

    codigoPlano = data.get('codigoPlano')
    arrayTipoNotas = data.get('arrayTipoNotas', '-')
    codEmpresa = data.get('codEmpresa', '1')


    dados = Plano.Plano(codigoPlano,'','','','','','',codEmpresa).desvincularNotasAoPlano(arrayTipoNotas)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)



@plano_routes.route('/pcp/api/VincularColecoesPlano', methods=['POST'])
@token_required
def post_VincularColecoesPlano():

    data = request.get_json()

    arrayColecao = data.get('arrayColecao')
    codPlano = data.get('codPlano')
    codEmpresa = data.get('codEmpresa', '1')


    dados = Plano.Plano(codPlano,'','','','','','',codEmpresa).vincularArrayColecaoPlano(arrayColecao)
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


@plano_routes.route('/pcp/api/DesvincularColecoesPlano', methods=['DELETE'])
@token_required
def Delete_DesvincularColecoesPlano():

    data = request.get_json()

    arrayColecao = data.get('arrayColecao')
    codPlano = data.get('codPlano')
    codEmpresa = data.get('codEmpresa', '1')


    dados =  Plano.Plano(codPlano,'','','','','','',codEmpresa).desvincularArrayColecaoPlano(arrayColecao)
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



@plano_routes.route('/pcp/api/InserirOuAlterPlanoABC', methods=['POST'])
@token_required
def post_InserirOuAlterPlanoABC():
    data = request.get_json()

    codPlano = data.get('codPlano')
    nomeABC = data.get('nomeABC')
    perc_dist = data.get('perc_dist')
    codEmpresa = data.get('codEmpresa','1')


    dados = Plano.Plano(codPlano,'','','','','','',codEmpresa,nomeABC).inserirOuAlterarPlanj_ABC(perc_dist)
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


