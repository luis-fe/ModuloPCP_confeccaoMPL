
from flask import Blueprint, jsonify, request, send_file
from functools import wraps
from src.models import Produtos
from io import BytesIO

produtos_routes = Blueprint('produtos_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function




@produtos_routes.route('/pcp/api/CategoriasDisponiveis', methods=['GET'])
@token_required
def get_CategoriasDisponiveis():

    dados = Produtos.Produtos('1').categoriasDisponiveis()
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


@produtos_routes.route('/pcp/api/obterColecao', methods=['GET'])
@token_required
def get_obterColecao():
    codItemPai = request.args.get('codItemPai','1')

    dados = Produtos.Produtos('1','','',codItemPai).codColceaoPai()
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



@produtos_routes.route('/pcp/api/obterImagemColorBook', methods=['GET'])
@token_required
def get_obterImagemColorBook():
    codItemPai = request.args.get('codItemPai','1')

    response = Produtos.Produtos('1','','',codItemPai).imagemColorBook()
    #controle.salvarStatus(rotina, ip, datainicio)


    return send_file(BytesIO(response.content), mimetype='image/jpeg')

