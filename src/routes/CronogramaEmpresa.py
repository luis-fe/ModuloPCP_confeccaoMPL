from flask import Blueprint, jsonify, request
from functools import wraps
import base64
import os

CronogramaEmpresa_routes = Blueprint('CronogramaEmpresa_routes', __name__)

# --- CORREÇÃO DE SEGURANÇA E ACESSO ---
# Salvamos dentro de 'static' para o navegador conseguir ler a imagem depois.
# O os.getcwd() garante que pegamos a raiz do projeto onde o app.py roda.
OUTPUT_FOLDER = os.path.join(os.getcwd(), "static", "imagens")

# Garante que a pasta existe (static/imagens_recebidas)
if not os.path.exists(OUTPUT_FOLDER):
    os.makedirs(OUTPUT_FOLDER)


def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        # DICA: Seu código VBA precisa enviar esse header, senão vai dar erro 401
        if token == 'a44pcp22':
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


# ... importações e setup inicial ...

@CronogramaEmpresa_routes.route('/upload', methods=['POST'])
# @token_required
def upload_image():
    try:
        data = request.get_json(force=True, silent=True)

        # Validação básica
        if not data or 'image' not in data:
            return jsonify({"status": "error", "message": "JSON inválido"}), 400

        image_data = data.get('image')

        # --- MUDANÇA AQUI: Ler o nome da aba enviado pelo Excel ---
        # Se não vier nome, usa 'Padrao' como segurança
        nome_aba = data.get('nome_aba', 'Padrao')

        # Limpa caracteres perigosos do nome (espaços, acentos, etc se quiser)
        nome_aba = nome_aba.strip()

        try:
            image_bytes = base64.b64decode(image_data)
        except Exception as e:
            return jsonify({"status": "error", "message": "Base64 inválido"}), 400

        # Agora o nome do arquivo é DINÂMICO baseado na aba
        filename = f"planilha_{nome_aba}.png"
        filepath = os.path.join(OUTPUT_FOLDER, filename)

        with open(filepath, "wb") as f:
            f.write(image_bytes)

        print(f"Recebido: {nome_aba} -> Salvo em: {filepath}")
        return jsonify({"status": "success", "file": filename}), 200

    except Exception as e:
        print(f"Erro Fatal: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500