from flask import Blueprint, jsonify, request
from functools import wraps
import base64
import os
from datetime import datetime

CronogramaEmpresa_routes = Blueprint('CronogramaEmpresa_routes', __name__)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function

# Cria a pasta para salvar as imagens se não existir
OUTPUT_FOLDER = "imagens_recebidas"
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)

@CronogramaEmpresa_routes.route('/upload', methods=['POST'])
def upload_image():
    try:
        data = request.json
        image_data = data.get('image') # O Excel vai mandar um JSON { "image": "..." }

        if not image_data:
            return jsonify({"status": "error", "message": "Nenhuma imagem recebida"}), 400

        # Decodifica o Base64
        image_bytes = base64.b64decode(image_data)

        # Gera nome único
        filename = f"planilha_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        # Salva o arquivo
        with open(filepath, "wb") as f:
            f.write(image_bytes)

        print(f"Imagem salva em: {filepath}")
        return jsonify({"status": "success", "file": filename}), 200

    except Exception as e:
        print(f"Erro: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
