import jaydebeapi
from contextlib import contextmanager
from dotenv import load_dotenv
import os
from src.configApp import configApp


@contextmanager
def ConexaoInternoMPL():
    """ Gerencia a conexão com o banco de dados usando JayDeBeApi """

    env_path = configApp.localProjeto
    load_dotenv(f'{env_path}/_ambiente.env')

    user = os.getenv('CSW_USER')
    password = os.getenv('CSW_PASSWORD')
    host = os.getenv('CSW_HOST')

    # 1. Pega o caminho exato da pasta onde ESTE arquivo (ConexaoERP.py) está rodando agora
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))

    # 2. Junta essa pasta com o nome do arquivo .jar (criando o caminho absoluto perfeito)
    caminho_jar = os.path.join(diretorio_atual, 'CacheDB.jar')

    # Adicione esta linha exata:
    print(f"DEBUG -> USER: {user} | HOST: {host} | SENHA: {'OK' if password else 'VAZIA'}", flush=True)

    conn = None

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}/CONSISTEM',
            {'user': user, 'password': password},
            caminho_jar  # <-- Colocamos a nossa variável inteligente aqui
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()