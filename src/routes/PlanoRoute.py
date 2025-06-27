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