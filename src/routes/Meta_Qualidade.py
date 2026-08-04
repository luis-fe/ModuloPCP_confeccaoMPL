from flask import Blueprint, jsonify, request
from functools import wraps
from src.service.Meta_Qualidade_service import Meta_Qualidade_Service

metaQualidade_routes = Blueprint('metaQualidade_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@metaQualidade_routes.route('/api/MetaQualidade', methods=['GET'])
@token_required
def get_MetaQualidade():
    '''
    Devolve as 12 metas do ano no formato consumido pela tela de Gestao da Qualidade:
        { "AnoMeta": 2026, "Meses": [12 nomes], "Meta": [12 fracoes] }
    A meta vai em fracao (0.015 = 1,50%).
    '''
    ano = request.args.get('ano', '-')

    dados = Meta_Qualidade_Service(ano).consultar_ano()

    return jsonify(dados)


@metaQualidade_routes.route('/api/MetaQualidade', methods=['POST'])
@token_required
def post_MetaQualidade():
    '''
    Grava as 12 metas do ano. Recebe o mesmo objeto "dados" que a tela envia ao
    requests.php:
        { "AnoMeta": 2026, "Meses": [12 nomes], "Meta": [12 fracoes] }
    Retorno: { "status": bool, "message": str, "dados": { ...formato do GET } }
    '''
    data = request.get_json(silent=True) or {}

    # Aceita tanto o objeto direto quanto encapsulado em "dados", como no requests.php
    dados = data.get('dados') if isinstance(data.get('dados'), dict) else data

    resposta = Meta_Qualidade_Service(dados.get('AnoMeta', '')).salvar_ano(dados)

    return jsonify(resposta)
