from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Parametrizacao_ABC

parametrizacaoABC_routes = Blueprint('parametrizacaoABC_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function





@parametrizacaoABC_routes.route('/pcp/api/consultaParametrizacaoABC', methods=['GET'])
@token_required
def get_consultaParametrizacaoABC():

    dados = Parametrizacao_ABC.Parametrizacao_ABC().consultaParametrizacaoABC()
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


@parametrizacaoABC_routes.route('/pcp/api/CadastrarParametroABC', methods=['POST'])
@token_required
def post_CadastrarParametroABC():
    data = request.get_json()

    parametroABC = data.get('parametroABC')


    dados = Parametrizacao_ABC.Parametrizacao_ABC('1',parametroABC).inserirParametroABC()
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
