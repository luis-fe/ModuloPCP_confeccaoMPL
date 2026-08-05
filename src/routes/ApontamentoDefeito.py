from flask import Blueprint, jsonify, request, send_file
from functools import wraps
from src.service.ApontamentoDefeito_service import ApontamentoDefeito_Service

ApontamentoDefeito_routes = Blueprint('ApontamentoDefeito_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


# Campos aceitos no corpo da requisicao - todos opcionais, default '-'
CAMPOS = ['dataApontamento', 'referencia', 'cor', 'tam', 'op', 'codTag',
          'usuario', 'motivoDefeito', 'detalhamento']

# Nomes aceitos para o arquivo enviado em multipart/form-data
CAMPOS_ARQUIVO = ['imagem', 'file', 'foto', 'arquivo']

# Nomes aceitos para a imagem enviada como base64 (json ou form)
CAMPOS_BASE64 = ['imagemBase64', 'imagem_base64', 'imagem64', 'base64']


def parametros_recebidos():
    '''
    Junta os parametros da query string, do form (multipart) e do json em um
    unico dict - a tela pode enviar de qualquer uma das tres formas.
    '''
    dados = dict(request.args)
    dados.update(request.form.to_dict())

    corpo = request.get_json(silent=True)
    if isinstance(corpo, dict):
        dados.update(corpo)

    return dados


def imagem_recebida(dados):
    '''Devolve o arquivo do multipart ou a string base64 enviada no corpo'''
    for campo in CAMPOS_ARQUIVO:
        if campo in request.files:
            return request.files[campo]

    for campo in CAMPOS_BASE64:
        if dados.get(campo):
            return dados[campo]

    return None


@ApontamentoDefeito_routes.route('/api/ApontamentoDefeito', methods=['POST'])
@token_required
def post_ApontamentoDefeito():
    '''
    Grava a imagem do defeito em /dados e registra o apontamento em
    pcp."ApntamentoDefeito".

    Parametros (query string, multipart/form-data ou json):
        tipoInformacao -> 'porTag' ou 'porOP' (obrigatorio, define o nome do arquivo)
        codTag         -> obrigatorio quando tipoInformacao = 'porTag'
        op             -> obrigatorio quando tipoInformacao = 'porOP'
        dataApontamento, referencia, cor, tam, usuario, motivoDefeito,
        detalhamento   -> opcionais, default '-'

    Imagem: arquivo no campo "imagem" (multipart) ou string base64 em "imagemBase64".
    Nome gravado: <codTag>_<motivoDefeito>.jpeg ou <op>_<motivoDefeito>.jpeg -
    quando a mesma chave + motivo ja tem foto, o nome ganha sufixo (_2, _3...)
    e cada apontamento vira um registro proprio.

    Retorno: { "status": bool, "message": str, "dados": { ...registro gravado } }
    '''
    dados = parametros_recebidos()

    resposta = ApontamentoDefeito_Service(dados.get('tipoInformacao', '-')).registrar(
        {campo: dados.get(campo, '-') for campo in CAMPOS},
        imagem_recebida(dados)
    )

    return jsonify(resposta), 200 if resposta['status'] else 400


@ApontamentoDefeito_routes.route('/api/ApontamentoDefeito', methods=['GET'])
@token_required
def get_ApontamentoDefeito():
    '''
    Consulta os apontamentos de defeito gravados. Todos os filtros sao opcionais:
        data_inicio, data_fim -> periodo de "dataApontamento" ('YYYY-MM-DD')
        op, codTag, referencia, cor, tam, usuario, motivoDefeito, dataApontamento,
        caminhoImg            -> filtros de igualdade
        textoAvancado         -> busca livre em referencia, motivo e detalhamento

    Cada registro traz o "caminhoImg" gravado, o "nomeArquivo" e o
    "imagemDisponivel" (se o arquivo ainda existe no volume).
    '''
    filtros = {
        'data_inicio': request.args.get('data_inicio', '-'),
        'data_fim': request.args.get('data_fim', '-'),
        'dataApontamento': request.args.get('dataApontamento', '-'),
        'referencia': request.args.get('referencia', '-'),
        'cor': request.args.get('cor', '-'),
        'tam': request.args.get('tam', '-'),
        'op': request.args.get('op', '-'),
        'codTag': request.args.get('codTag', '-'),
        'usuario': request.args.get('usuario', '-'),
        'motivoDefeito': request.args.get('motivoDefeito', '-'),
        'caminhoImg': request.args.get('caminhoImg', '-'),
        'textoAvancado': request.args.get('textoAvancado', '')
    }

    dados = ApontamentoDefeito_Service().consultar(filtros)

    return jsonify(dados)


@ApontamentoDefeito_routes.route('/api/ApontamentoDefeitoImagem', methods=['GET'])
@token_required
def get_ApontamentoDefeitoImagem():
    '''
    Devolve a foto do defeito. Aceita o "caminhoImg" retornado na consulta ou a
    combinacao tipoInformacao + codTag/op + motivoDefeito.
    '''
    dados = {
        'caminhoImg': request.args.get('caminhoImg', '-'),
        'codTag': request.args.get('codTag', '-'),
        'op': request.args.get('op', '-'),
        'motivoDefeito': request.args.get('motivoDefeito', '-')
    }

    resposta = ApontamentoDefeito_Service(
        request.args.get('tipoInformacao', '-')
    ).caminho_imagem(dados)

    if not resposta['status']:
        return jsonify(resposta), 404

    return send_file(resposta['caminho'], mimetype='image/jpeg')


@ApontamentoDefeito_routes.route('/api/ApontamentoDefeito', methods=['PUT'])
@token_required
def put_ApontamentoDefeito():
    '''
    Atualiza os campos de um apontamento ja gravado, identificado pelo "caminhoImg".
    A imagem nao muda aqui - para trocar a foto, exclua o apontamento e grave outro.
    '''
    dados = parametros_recebidos()

    resposta = ApontamentoDefeito_Service().atualizar(
        dados.get('caminhoImg', '-'),
        {campo: dados[campo] for campo in CAMPOS if campo in dados}
    )

    return jsonify(resposta), 200 if resposta['status'] else 400


@ApontamentoDefeito_routes.route('/api/ApontamentoDefeito', methods=['DELETE'])
@token_required
def delete_ApontamentoDefeito():
    '''
    Exclui o apontamento identificado pelo "caminhoImg" e, por default, tambem a
    imagem do volume (envie excluirImagem = 'false' para manter o arquivo).
    '''
    dados = parametros_recebidos()

    excluirImagem = str(dados.get('excluirImagem', 'true')).strip().lower() not in ('false', '0', 'nao', 'não')

    resposta = ApontamentoDefeito_Service().excluir(dados.get('caminhoImg', '-'), excluirImagem)

    return jsonify(resposta), 200 if resposta['status'] else 400
