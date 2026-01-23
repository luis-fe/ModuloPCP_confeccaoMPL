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
        # 1. Tenta pegar o JSON forçando a leitura (ignora se o Content-Type estiver errado)
        data = request.get_json(force=True, silent=True)

        # 2. Se o JSON veio vazio ou inválido, tenta debugar
        if not data:
            print("\n--- ERRO: JSON não reconhecido ---")
            print(f"Cabeçalhos recebidos: {request.headers}")
            # Lê os primeiros 100 caracteres para ver se veio lixo
            raw_preview = request.data[:100]
            print(f"Início dos dados brutos: {raw_preview}")
            return jsonify({"status": "error", "message": "JSON inválido ou vazio"}), 400

        image_data = data.get('image')

        if not image_data:
            return jsonify({"status": "error", "message": "Campo 'image' não encontrado"}), 400

        # 3. Decodifica
        try:
            image_bytes = base64.b64decode(image_data)
        except Exception as e:
            return jsonify({"status": "error", "message": f"Base64 inválido: {str(e)}"}), 400

        # 4. Salva
        filename = f"planilha_{'Slide1'}.png"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        with open(filepath, "wb") as f:
            f.write(image_bytes)

        print(f"Sucesso! Imagem salva: {filepath}")
        return jsonify({"status": "success", "file": filename}), 200

    except Exception as e:
        # Pega erros internos do servidor e imprime no terminal
        print(f"\n--- ERRO FATAL 500 ---")
        print(e)
        return jsonify({"status": "error", "message": str(e)}), 500
