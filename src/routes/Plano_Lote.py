from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Plano_Lote

planoLote_routes = Blueprint('planoLote_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@planoLote_routes.route('/pcp/api/VincularLotesPlano', methods=['PUT'])
@token_required
def put_VincularLotesPlano():

    data = request.get_json()
    codEmpresa = data.get('codEmpresa', '1')
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')


    dados = Plano_Lote.Plano_Lote(codEmpresa, codigoPlano).vincularLotesAoPlano(arrayCodLoteCsw)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)

@planoLote_routes.route('/pcp/api/DesvincularLotesPlano', methods=['DELETE'])
@token_required
def Delete_DesvincularLotesPlano():

    data = request.get_json()

    codEmpresa = data.get('codEmpresa', '1')
    codigoPlano = data.get('codigoPlano')
    arrayCodLoteCsw = data.get('arrayCodLoteCsw', '-')


    dados = Plano_Lote.Plano_Lote('1', codigoPlano).desvincularLotesAoPlano(arrayCodLoteCsw)
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)



