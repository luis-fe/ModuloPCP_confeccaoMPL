
from flask import Blueprint, jsonify, request, send_file
from functools import wraps
from src.models import Produtos, SubstitutoClass
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


@produtos_routes.route('/pcp/api/colecao_csw', methods=['GET'])
@token_required
def get_colecao_csw():


    dados = Produtos.Produtos('1').obterColecaoCsw()
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


@produtos_routes.route('/pcp/api/obterImagemSColorBook', methods=['GET'])
def get_obterImagemSColorBook():
    codItemPai = request.args.get('codItemPai','1')
    indice = int(request.args.get('indice', '0'))


    response = Produtos.Produtos('1','','',codItemPai, indice).imagem_S_ColorBook()
    #controle.salvarStatus(rotina, ip, datainicio)

    return jsonify(response)
@produtos_routes.route('/pcp/api/MarcasDisponiveis', methods=['GET'])
@token_required
def get_MarcasDisponiveis():

    dados = Produtos.Produtos().consultaMarcasDisponiveis()
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



@produtos_routes.route('/pcp/api/consultaSubstitutosMP', methods=['GET'])
@token_required
def get_consultaSubstitutosMP():

    codEmpresa = request.args.get('codEmpresa','1')


    dados = SubstitutoClass.Substituto('','','','',codEmpresa).consultaSubstitutos()

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


@produtos_routes.route('/pcp/api/inserirAlterarSubstitutosMP', methods=['POST'])
@token_required
def post_inserirAlterarSubstitutosMP():
    data = request.get_json()

    codMateriaPrima = data.get('codMateriaPrima')
    nomeCodMateriaPrima = data.get('nomeCodMateriaPrima')
    codMateriaPrimaSubstituto = data.get('codMateriaPrimaSubstituto')
    nomeCodSubstituto = data.get('nomeCodSubstituto')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = SubstitutoClass.Substituto(codMateriaPrima,codMateriaPrimaSubstituto,nomeCodMateriaPrima,nomeCodSubstituto,codEmpresa).inserirOuAlterSubstitutoMP()

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

