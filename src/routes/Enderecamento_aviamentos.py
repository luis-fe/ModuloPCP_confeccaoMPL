from flask import Blueprint, jsonify, request
from functools import wraps
from src.service import Enderecamento_aviamentos_service, Conferencia_itens_separados

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


@Enderecamento_routes.route('/pcp/api/Fila_recebimento_Aviamentos', methods=['GET'])
@token_required
def get_Fila_recebimento_Aviamentos():
    codEmpresa = request.args.get('codEmpresa','1')

    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa).fila_itens_enderecar()
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



@Enderecamento_routes.route('/pcp/api/FilaConferencia', methods=['GET'])
@token_required
def get_Fila_conferencia():
    codEmpresa = request.args.get('codEmpresa','1')

    dados = Conferencia_itens_separados.Conferencia_itens_separados(codEmpresa).carregar_ordens_para_conferencia()
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


@Enderecamento_routes.route('/pcp/api/obterNomeItem', methods=['GET'])
@token_required
def get_obterNomeItem():
    codMaterial = request.args.get('codMaterial','1')

    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento('','','','','','','',codMaterial).procurar_nome_item_considear()
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


@Enderecamento_routes.route('/pcp/api/ItensConferencia', methods=['GET'])
@token_required
def get_ItensConferencia():
    codEmpresa = request.args.get('codEmpresa','1')
    numeroOP = request.args.get('numeroOP','1')

    dados = Conferencia_itens_separados.Conferencia_itens_separados(codEmpresa,'',numeroOP).carregar_itens_para_conferir()
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


@Enderecamento_routes.route('/pcp/api/get_obter_itens_configurados', methods=['GET'])
@token_required
def get_obter_itens_configurados():
    codEmpresa = request.args.get('codEmpresa','1')

    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa,'','','','','','').get_obter_itens_configurados()
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


@Enderecamento_routes.route('/pcp/api/procurar_nome_item_considear', methods=['GET'])
@token_required
def get_procurar_nome_item_considear():
    codEmpresa = request.args.get('codEmpresa','1')
    codMaterial = request.args.get('codMaterial','1')

    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa,'','','','','','',codMaterial).procurar_nome_item_considerar()
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
def POST_inserir_endereco():
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


@Enderecamento_routes.route('/pcp/api/inserir_material_desconsiderar_conf', methods=['POST'])
@token_required
def POST_inserir_material_desconsiderar_conf():
    data = request.get_json()


    codMaterial = data.get('codMaterial','')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa, '','','','','','',codMaterial).inserirItemDesconsiderar()

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


@Enderecamento_routes.route('/pcp/api/conferenciaAviamentos_', methods=['POST'])
@token_required
def POST_conferenciaAviamentos_():
    data = request.get_json()


    codEmpresa = data.get('codEmpresa','1')
    codMaterial = data.get('codMaterial','1')
    numeroOP = data.get('numeroOP','1')


    dados = Conferencia_itens_separados.Conferencia_itens_separados(codEmpresa, codMaterial, numeroOP,).inserir_conferencia()

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


    rua = data.get('rua','')
    quadra = data.get('quadra','')
    posicao = data.get('posicao','')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa, rua, quadra, posicao,'','','').inserir_endereco()

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


@Enderecamento_routes.route('/pcp/api/inserir_endereco_aviamento_em_massa', methods=['POST'])
@token_required
def POST_inserir_endereco_aviamento_em_massa():
    data = request.get_json()

    ruaInicial = data.get('ruaInicial')
    posicaoInicial = data.get('posicaoInicial','')
    quadraInicial = data.get('quadraInicial','')
    codEmpresa = request.args.get('codEmpresa','1')
    ruaFinal = data.get('ruaFinal')
    posicaoFinal = data.get('posicaoFinal','')
    quadraFinal = data.get('quadraFinal','')


    dados = Enderecamento_aviamentos_service.Enderecamento_aviamento(codEmpresa, ruaInicial, quadraInicial, posicaoInicial,ruaFinal,quadraFinal,posicaoFinal).inserir_endereco_massa()

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


