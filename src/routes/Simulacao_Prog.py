from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import SimulacaoProg

Simulacao_prod_routes = Blueprint('Simulacao_prod_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@Simulacao_prod_routes.route('/pcp/api/ConsultaSimulacoes', methods=['GET'])
@token_required
def get_ConsultaSimulacoes():

    dados = SimulacaoProg.SimulacaoProg().get_Simulacoes()
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


@Simulacao_prod_routes.route('/pcp/api/consultaDetalhadaSimulacao', methods=['GET'])
@token_required
def get_consultaDetalhadaSimulacao():

    nomeSimulacao = request.args.get('nomeSimulacao')

    dados = SimulacaoProg.SimulacaoProg(nomeSimulacao).consultaDetalhadaSimulacao()
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

@Simulacao_prod_routes.route('/pcp/api/atualizaInserirSimulacao', methods=['POST'])
@token_required
def post_atualizaInserirSimulacao():

    data = request.get_json()
    nomeSimulacao = data.get('nomeSimulacao')
    arrayAbc = data.get('arrayAbc',[])
    arrayCategoria = data.get('arrayCategoria',[])
    arrayMarca = data.get('arrayMarca',[])
    empresa = data.get('empresa','1')



    dados = SimulacaoProg.SimulacaoProg(nomeSimulacao,'','','','',empresa).inserirAtualizarSimulacao(arrayAbc, arrayMarca,arrayCategoria)

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



@Simulacao_prod_routes.route('/pcp/api/atualizaInserirSimulacaoProdutos', methods=['POST'])
@token_required
def post_atualizaInserirSimulacaoProdutos():

    data = request.get_json()
    nomeSimulacao = data.get('nomeSimulacao')
    arrayProdutos = data.get('arrayProdutos',[])
    arrayPercentual = data.get('arrayPercentual',[])
    empresa = data.get('empresa','1')



    dados = SimulacaoProg.SimulacaoProg(nomeSimulacao,'','','','',empresa).simulacaoProdutos_tendencia(arrayProdutos, arrayPercentual)

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


@Simulacao_prod_routes.route('/pcp/api/deletarSimulacao', methods=['DELETE'])
@token_required
def delete_deletarSimulacao():

    data = request.get_json()
    nomeSimulacao = data.get('nomeSimulacao')


    dados = SimulacaoProg.SimulacaoProg(nomeSimulacao).excluirSimulacao()

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


@Simulacao_prod_routes.route('/pcp/api/limpar_produtos_simulacao_Especifica', methods=['DELETE'])
@token_required
def limpar_produtos_simulacao_Especifica():

    data = request.get_json()
    nomeSimulacao = data.get('nomeSimulacao')
    empresa = data.get('empresa','1')


    dados = SimulacaoProg.SimulacaoProg(nomeSimulacao,'','','','',empresa).limpar_produtos_simulacao_Especifica()

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
