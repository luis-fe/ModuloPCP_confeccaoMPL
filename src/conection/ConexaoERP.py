import jaydebeapi
from contextlib import contextmanager
from dotenv import load_dotenv
import os

from src.configApp import configApp
from src.models import OP_CSW


@contextmanager
def ConexaoInternoMPL():
    """ Gerencia a conexão com o banco de dados usando JayDeBeApi """

    env_path = configApp.localProjeto
    # Carregar variáveis de ambiente do arquivo .env
    load_dotenv(f'{env_path}/_ambiente.env')

    # Obter valores das variáveis
    user = os.getenv('CSW_USER')
    password = os.getenv('CSW_PASSWORD')
    host = os.getenv('CSW_HOST')

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}/CONSISTEM',
            {'user': f'{user}', 'password': f'{password}'},
            f'./src/connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()

####### TESTE NO INICIO DA APLICACAO,
env_path = configApp.localProjeto
# Carregar variáveis de ambiente do arquivo .env
load_dotenv(f'{env_path}/_ambiente.env')
user = os.getenv('CSW_USER')
password = os.getenv('CSW_PASSWORD')
host = os.getenv('CSW_HOST')
print(f'Teste variavel {host}/{password}/')
try:
    teste = OP_CSW.OP_CSW().obterTiposOPCSW()
    print(f' {teste["tipoOP"][0]}')
except:
    print('caiu a conexao')

