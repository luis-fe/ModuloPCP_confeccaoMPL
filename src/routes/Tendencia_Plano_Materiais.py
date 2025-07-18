from flask import Blueprint, jsonify, request, make_response, send_file
from functools import wraps
import io
import jpype

import src.connection.ConexaoERP
from src.models import Tendencia_Plano_Materiais, Produtos

Tendencia_Plano_Materiais_routes = Blueprint('Tendencia_Plano_Materiais_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

@Tendencia_Plano_Materiais_routes.route('/pcp/api/AnaliseMateriaisPelaTendencia', methods=['POST'])
@token_required
def post_AnaliseMateriaisPelaTendencia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codEmpresa = data.get('codEmpresa','1')
    congelar =  data.get('congelar',False)
    print(data)

    if congelar == False:
        dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq).estruturaItens('nao','nao','nao')
    else:
        dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq).estrutura_ItensCongelada()

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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/AnaliseMateriaisPelaSimulacao', methods=['POST'])
@token_required
def post_AnaliseMateriaisPelaSimulacao():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codEmpresa = data.get('codEmpresa','1')
    nomeSimulacao = data.get('nomeSimulacao')


    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq,'','',nomeSimulacao).estrutura_ItensCongelada('sim')
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


@Tendencia_Plano_Materiais_routes.route('/pcp/api/obtendoUltimaAnalise_porPlano', methods=['GET'])
@token_required
def obtendoUltimaAnalise_porPlano():

    codPlano = request.args.get('codPlano','-')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa,codPlano).obtendoUltimaAnalise_porPlano()
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



@Tendencia_Plano_Materiais_routes.route('/pcp/api/CalculoPcs_baseaado_MP', methods=['POST'])
@token_required
def post_CalculoPcs_baseaado_MP():
    data = request.get_json()


    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codEmpresa = data.get('codEmpresa','1')
    arrayCategoriaMP = data.get('arrayCategoriaMP','')
    nomeSimulacao = data.get("nomeSimulacao",'nao')

    print(data)

    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa, codPlano, consideraPedBloq,'','',nomeSimulacao).calculoIdealPcs_para_materiaPrima(nomeSimulacao,arrayCategoriaMP)
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


@Tendencia_Plano_Materiais_routes.route('/pcp/api/naturezaEstoqueComponentes', methods=['GET'])
@token_required
def ger_naturezaEstoqueComponentes():

    codEmpresa = request.args.get('codEmpresa','1')

    dados = Produtos.Produtos(codEmpresa).estMateriaPrima_nomes()
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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/comprometidoOP', methods=['GET'])
@token_required
def ger_comprometidoOP():

    codEmpresa = request.args.get('codEmpresa','1')

    dados = Produtos.Produtos(codEmpresa).estoqueComprometido()

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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/comprometidoCompras', methods=['GET'])
@token_required
def ger_comprometidoCompras():
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Produtos.Produtos(codEmpresa).estoquePedidosCompras()

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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/DetalhaNecessidade', methods=['POST'])
@token_required
def post_DetalhaNecessidade():
    data = request.get_json()

    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codComponente = data.get('codComponente')
    nomeSimulacao = data.get("nomeSimulacao",'nao')

    if nomeSimulacao == 'nao':
        nomeSimulacao = ''


    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais('1',codPlano, consideraPedBloq,'',
                                                                str(codComponente),nomeSimulacao).detalhaNecessidade(nomeSimulacao)
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


@Tendencia_Plano_Materiais_routes.route('/pcp/api/categroriaMP', methods=['GET'])
@token_required
def get_categroriaMP():

    codEmpresa = request.args.get('codEmpresa','1')

    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa).obter_categoriasMP()
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

@Tendencia_Plano_Materiais_routes.route('/pcp/api/detalharSku_x_AnaliseEmpenho', methods=['POST'])
@token_required
def post_detalharSku_x_AnaliseEmpenhoe():
    data = request.get_json()

    codEmpresa = data.get('codEmpresa','1')
    codPlano = data.get('codPlano')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    codReduzido = data.get('codReduzido')
    nomeSimulacao = data.get("nomeSimulacao",'nao')
    arrayCategoriaMP = data.get('arrayCategoriaMP','')

    if nomeSimulacao == '':
        nomeSimulacao = 'nao'



    dados = Tendencia_Plano_Materiais.Tendencia_Plano_Materiais(codEmpresa,codPlano, consideraPedBloq,'','',nomeSimulacao,
                                                                str(codReduzido)).detalharSku_x_AnaliseEmpenho(nomeSimulacao, arrayCategoriaMP)
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



@Tendencia_Plano_Materiais_routes.route("/imagem/<string:cpf>", defaults={'indice': 0})
@Tendencia_Plano_Materiais_routes.route("/imagem/<string:cpf>/<int:indice>")
def obter_imagem(cpf, indice):
    try:
        imagens_bytes = []

        with src.connection.ConexaoERP.ConexaoInternoMPL() as conn:
            cursor = conn.cursor()
            sql = f"""
                SELECT stream FROM Utils_Persistence.Csw1Stream 
                WHERE 
                    rotinaAcesso = '%CSWANEXO' 
                    AND documentoReferencia LIKE 'Item-{cpf}%'
                    AND stream is not null                
                    ORDER BY nomeArquivo
            """
            cursor.execute(sql)
            rows = cursor.fetchall()

            for row in rows:
                if row and row[0]:
                    java_stream = row[0]

                    # Criar buffer de leitura
                    JByteArray = jpype.JArray(jpype.JByte)
                    buffer = JByteArray(4096)

                    bytes_data = bytearray()
                    read_len = java_stream.read(buffer)

                    while read_len != -1:
                        bytes_data.extend(buffer[:read_len])
                        read_len = java_stream.read(buffer)

                    imagens_bytes.append(bytes(bytes_data))

        total_imagens = len(imagens_bytes)

        if total_imagens == 0:
            return make_response("Nenhuma imagem encontrada.", 404)

        if indice >= total_imagens or indice < 0:
            return make_response(f"Índice inválido. Existem {total_imagens} imagens disponíveis.", 400)

        imagem_escolhida = imagens_bytes[indice]

        response = send_file(
            io.BytesIO(imagem_escolhida),
            mimetype='image/jpeg',
            as_attachment=False,
            download_name=f"{cpf}_{indice + 1}.jpg"
        )

        # Cabeçalho customizado indicando total de imagens
        response.headers["X-Total-Imagens"] = str(total_imagens)
        response.headers["X-Imagem-Atual"] = str(indice + 1)

        return response

    except Exception as e:
        return make_response(f"Erro: {str(e)}", 500)


@Tendencia_Plano_Materiais_routes.route("/imagem/<string:cpf>/quantidade")
def obter_quantidade_imagens(cpf):
    try:
        with src.connection.ConexaoERP.ConexaoInternoMPL() as conn:
            cursor = conn.cursor()
            sql = f"""
                SELECT COUNT(*) FROM Utils_Persistence.Csw1Stream 
                WHERE 
                    rotinaAcesso = '%CSWANEXO' 
                    AND documentoReferencia LIKE 'Item-{cpf}%'
                    AND stream is not null
            """
            cursor.execute(sql)
            count = cursor.fetchone()[0]
        return {"total_imagens": count}
    except Exception as e:
        return make_response(f"Erro: {str(e)}", 500)


