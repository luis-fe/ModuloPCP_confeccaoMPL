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

    conn = None  # <- IMPORTANTE

    try:
        conn = jaydebeapi.connect(
            'com.intersys.jdbc.CacheDriver',
            f'jdbc:Cache://{host}/CONSISTEM',
            {'user': user, 'password': password},
            './src/connection/CacheDB.jar'
        )
        yield conn
    finally:
        if conn is not None:
            conn.close()


####### TESTE NO INICIO DA APLICACAO,

