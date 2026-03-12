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

    conn = None

    # --- INÍCIO DA MUDANÇA ---
    # Pega o caminho absoluto da pasta onde este arquivo (ConexaoERP.py) está:
    diretorio_atual = os.path.dirname(os.path.abspath(__file__))

    # Junta o caminho da pasta com o nome do arquivo .jar:
    caminho_jar = os.path.join(diretorio_atual, 'CacheDB.jar')
    # --- FIM DA MUDANÇA ---

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}/CONSISTEM',
            {'user': user, 'password': password},
            caminho_jar  # <-- Passamos a variável segura aqui
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()