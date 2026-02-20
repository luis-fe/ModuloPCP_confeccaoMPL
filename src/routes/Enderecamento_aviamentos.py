from flask import Blueprint, jsonify, request
from functools import wraps
from src.service import Enderecamento_aviamentos_service

Enderecamento_routes = Blueprint('Enderecamento_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@Enderecamento_routes.route('/pcp/api/get_enderecos', methods=['GET'])
@token_required
def get_enderecos():
    codEmpresa = request.args.get('codEmpresa','1')

    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa).get_enderecos()
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

@Enderecamento_routes.route('/pcp/api/inserir_endereco_aviamento', methods=['POST'])
@token_required
def POST_inserir_endereco_aviamento():
    data = request.get_json()

    rua = data.get('rua')
    posicao = data.get('posicao','')
    quadra = data.get('quadra','')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa, rua, quadra, posicao).inserir_endereco()

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


