from flask import Blueprint, jsonify, request, make_response, send_file
from functools import wraps
import io
import jpype
import src.connection.ConexaoERP
from src.models import Tendencia_Plano

Tendencia_Plano_routes = Blueprint('Tendencia_Plano_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@Tendencia_Plano_routes.route('/pcp/api/consultaPlanejamentoABC_plano', methods=['GET'])
@token_required
def get_consultaPlanejamentoABC_plano():

    codPlano = request.args.get('codPlano','-')
    codEmpresa = request.args.get('codEmpresa','-')

    dados = Tendencia_Plano.Tendencia_Plano(codEmpresa,codPlano).consultaPlanejamentoABC()
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




@Tendencia_Plano_routes.route('/pcp/api/ABCReferencia', methods=['POST'])
@token_required
def post_ABCReferencia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')


    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaAbc()

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


@Tendencia_Plano_routes.route('/pcp/api/tendenciaSku', methods=['POST'])
@token_required
def post_tendenciaSku():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    congelar = data.get('congelar',False)

    if congelar == False:
        dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaVendas()
    else:
        print(data)
        dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaCongeladSku()

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


@Tendencia_Plano_routes.route('/pcp/api/tendenciaResumoEngenharia', methods=['POST'])
@token_required
def post_tendenciaResumoEngenharia():
    data = request.get_json()

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')

    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq).tendenciaResumoEngharia()

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


@Tendencia_Plano_routes.route('/pcp/api/obtendoUltimaTendencia_porPlano', methods=['GET'])
@token_required
def obtendoUltimaTendencia_porPlano():

    codPlano = request.args.get('codPlano','-')
    codEmpresa = request.args.get('codEmpresa','1')


    dados = Tendencia_Plano.Tendencia_Plano(codEmpresa,codPlano).obtendoUltimaTendencia()
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

@Tendencia_Plano_routes.route('/pcp/api/simulacaoProgramacao', methods=['POST'])
@token_required
def post_simulacaoProgramacao():
    '''Api que simulaca a "tendendcia de vendas baseado" no modelo montando pelo usuario '''
    data = request.get_json()
    print(data)
    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    nomeSimulacao = data.get('nomeSimulacao')

    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq,nomeSimulacao).simulacaoPeloNome()
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


@Tendencia_Plano_routes.route('/pcp/api/simulacaoDetalhadaPorSku', methods=['POST'])
@token_required
def post_simulacaoDetalhadaPorSku():
    data = request.get_json()
    print(data)

    codPlano = data.get('codPlano')
    empresa = data.get('empresa','1')
    consideraPedBloq = data.get('consideraPedBloq','nao')
    nomeSimulacao = data.get('nomeSimulacao','')
    codSku = data.get('codSku')

    dados = Tendencia_Plano.Tendencia_Plano(empresa, codPlano,consideraPedBloq,nomeSimulacao, codSku).detalhaCalculoPrev()
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


@Tendencia_Plano_routes.route("/imagemEng/<string:eng>", defaults={'indice': 0})
@Tendencia_Plano_routes.route("/imagemEng/<string:eng>/<int:indice>")
def obter_imagemEng(eng, indice):
    try:
        imagens_bytes = []

        with src.connection.ConexaoERP.ConexaoInternoMPL() as conn:
            cursor = conn.cursor()
            sql = f"""
                SELECT stream FROM Utils_Persistence.Csw1Stream 
                WHERE 
                     documentoReferencia LIKE 'Eng-{eng}%'
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
            download_name=f"{eng}_{indice + 1}.jpg"
        )

        # Cabeçalho customizado indicando total de imagens
        response.headers["X-Total-Imagens"] = str(total_imagens)
        response.headers["X-Imagem-Atual"] = str(indice + 1)

        return response

    except Exception as e:
        return make_response(f"Erro: {str(e)}", 500)


@Tendencia_Plano_routes.route("/imagemEng/<string:eng>/quantidade")
def obter_quantidade_imagensEngenharia(eng):
    try:
        with src.connection.ConexaoERP.ConexaoInternoMPL() as conn:
            cursor = conn.cursor()
            sql = f"""
                SELECT COUNT(*) FROM Utils_Persistence.Csw1Stream 
                WHERE 
                    documentoReferencia LIKE 'Eng-{eng}%'
                    AND stream is not null
            """
            cursor.execute(sql)
            count = cursor.fetchone()[0]
        return {"total_imagens": count}
    except Exception as e:
        return make_response(f"Erro: {str(e)}", 500)