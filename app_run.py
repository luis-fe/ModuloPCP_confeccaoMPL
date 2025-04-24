from flask import Flask
import os

from src.configApp import configApp
from src.routes import routes_blueprint  # Certifique-se de que 'routes.py' está na mesma pasta
from dotenv import load_dotenv

app = Flask(__name__)
env_path = configApp.localProjeto
# Carregar variáveis de ambiente do arquivo .env
load_dotenv(f'{env_path}/_ambiente.env')
porta_escolhida = os.getenv('PORTA_APLICACAO')

port = int(os.environ.get('PORT', int(porta_escolhida)))

# Registrar o Blueprint corretamente
app.register_blueprint(routes_blueprint)

if __name__ == '__main__':



    app.run(host='0.0.0.0', port=port)
