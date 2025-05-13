'''Arquivo para chamar a conexao de banco com o postgre'''
import os

import pandas as pd
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine
from src.configApp import configApp

env_path = configApp.localProjeto
# Carregar variáveis de ambiente do arquivo .env
load_dotenv(f'{env_path}/_ambiente.env')

def conexaoEngine():
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')


    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}"
    return create_engine(connection_string)

def conexaoEngineWMSSrv():
    db_name = os.getenv('POSTGRES_DB2')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV2')
    db_host = os.getenv('POSTGRES_HOST_SRV2')
    db_porta = os.getenv('POSTGRES_PORT')


    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}"
    return create_engine(connection_string)

def conexaoEngineWms():
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')


    if not all([db_name, db_user, db_password, db_host]):
        raise ValueError("One or more environment variables are not set")

    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}"
    return create_engine(connection_string)

def Funcao_InserirOFF (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')


# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')

def Funcao_InserirOFF_srvWMS (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV2')
    db_host = os.getenv('POSTGRES_HOST_SRV2')
    db_porta = os.getenv('POSTGRES_PORT')

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='pcp')

def conexaoInsercao():
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')

    return psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_porta)


def Funcao_InserirBackup (df_tags, tamanho,tabela, metodo):
    # Configurações de conexão ao banco de dados
    db_name = os.getenv('POSTGRES_DB')
    db_user = os.getenv('POSTGRES_USER')
    db_password = os.getenv('POSTGRES_PASSWORD_SRV1')
    db_host = os.getenv('POSTGRES_HOST_SRV1')
    db_porta = os.getenv('POSTGRES_PORT')

# Cria conexão ao banco de dados usando SQLAlchemy
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_porta}/{db_name}')

    # Inserir dados em lotes
    chunksize = tamanho
    for i in range(0, len(df_tags), chunksize):
        df_tags.iloc[i:i + chunksize].to_sql(tabela, engine, if_exists=metodo, index=False , schema='backup')

