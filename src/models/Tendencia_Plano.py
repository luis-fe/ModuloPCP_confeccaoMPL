import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.conection import ConexaoPostgre
from src.models import Pedidos


class Tendencia_Plano():
    """Classe que gerencia o processo de Calculo de Tendencia de um plano """

    def __init__(self, codEmpresa = '1', codPlano = '', consideraPedBloq = ''):
        '''Contrutor da classe '''
        self.codEmpresa = codEmpresa
        self.codPlano = codPlano
        self.consideraPedBloq = consideraPedBloq


    def consultaPlanejamentoABC(self):
        '''Metodo utilizado para planejar a distribuicacao ABC'''

        sql = """
        Select "nomeABC" , "perc_dist" from pcp."Plano_ABC"
        where 
            "codPlano" = %s
        order by 
            "nomeABC"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        consulta['perc_Acumulado'] = consulta['perc_dist'].cumsum()


        totalDistribuido = consulta['perc_dist'].sum()
        faltaDistribuir = 100 - totalDistribuido

        data = {
                '1- Total Distribuido': f'{totalDistribuido}%',
                '2- Falta Distribuir':f'{faltaDistribuir}%',
                '3- Detalhamento:': consulta.to_dict(orient='records')
            }
        return pd.DataFrame([data])



    def tendenciaAbc(self, utilizaCongelento = 'nao'):
        '''Metodo que retorna a tendencia ABC '''

        # 1 - Verifica se é para utilizar ou nao o congelamento ABC que tem como premisa agilar a consulta quanto utilizado na tendenciaSku
        if utilizaCongelento == 'nao':
            vendas = Pedidos.Pedidos(self.codEmpresa, self.codPlano,  self.consideraPedBloq)
            consultaVendasSku = vendas.listagemPedidosSku()
            consultaVendasSku = consultaVendasSku[consultaVendasSku['categoria'] != 'SACOLA']

        else:
            load_dotenv('db.env')
            caminhoAbsoluto = os.getenv('CAMINHO')
            consultaVendasSku = pd.read_csv(f'{caminhoAbsoluto}/dados/tenendicaPlano-{self.codPlano}.csv')

        consultaVendasSku = consultaVendasSku.groupby(["codItemPai"]).agg({"marca": "first",
                                                                           "nome": 'first',
                                                                           "categoria": 'first',
                                                                           "qtdePedida": "sum",
                                                                           "qtdeFaturada": 'sum',
                                                                           "valorVendido": 'sum'}).reset_index()
        consultaVendasSku = consultaVendasSku.sort_values(by=['qtdePedida'],
                                                          ascending=False)  # escolher como deseja classificar


        consultaVendasSku['totalVendas'] = consultaVendasSku.groupby('marca')['qtdePedida'].transform('sum')
        consultaVendasSku['totalVendasCategoria'] = consultaVendasSku.groupby(['marca','categoria'])['qtdePedida'].transform('sum')


        consultaVendasSku['ABCdist%'] = np.where(
            consultaVendasSku['qtdePedida'] == 0,  # Condição
            0,  # Valor se condição for verdadeira
            consultaVendasSku['qtdePedida'] / consultaVendasSku['totalVendas']  # Valor se falsa
        )

        consultaVendasSku['ABCdist%Categoria'] = np.where(
            consultaVendasSku['qtdePedida'] == 0,  # Condição
            0,  # Valor se condição for verdadeira
            consultaVendasSku['qtdePedida'] / consultaVendasSku['totalVendasCategoria']  # Valor se falsa
        )


        consultaVendasSku['nome'] = consultaVendasSku['nome'].str.rsplit(' ', n=2).str[:-1].str.join(' ')
        consultaVendasSku['nome'] = consultaVendasSku['nome'].str.rsplit(' ', n=2).str[:-1].str.join(' ')
        consultaVendasSku['ABC_Acum%'] = consultaVendasSku.groupby('marca')['ABCdist%'].cumsum()
        consultaVendasSku['ABC_Acum%Categoria'] = consultaVendasSku.groupby(['marca','categoria'])['ABCdist%Categoria'].cumsum()


        consultaVendasSku['ABC_Acum%'] = consultaVendasSku['ABC_Acum%'].round(4)
        consultaVendasSku['ABC_Acum%Categoria'] = consultaVendasSku['ABC_Acum%Categoria'].round(4)


        # Consultando o ABC cadastrado para o Plano:
        sql = """
        Select "nomeABC" , "perc_dist", "codPlano" from pcp."Plano_ABC"
        where 
            "codPlano" = %s
        order by 
            "nomeABC"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        consulta['perc_dist'] = consulta['perc_dist'].cumsum()
        consulta['perc_dist'] = consulta['perc_dist']/100
        # Adiciona os limites inferiores das faixas
        bins = [0] + consulta['perc_dist'].tolist()  # [0, 20, 50, 100]
        labels = consulta['nomeABC'].tolist()  # ['a', 'b', 'c']

        # Classifica cada percentual de vendas nas faixas definidas
        consultaVendasSku['class'] = pd.cut(
            consultaVendasSku['ABC_Acum%'],
            bins=bins,
            labels=labels,
            include_lowest=True
        )

        consultaVendasSku['classCategoria'] = pd.cut(
            consultaVendasSku['ABC_Acum%Categoria'],
            bins=bins,
            labels=labels,
            include_lowest=True
        )

        consultaVendasSku['class'] = consultaVendasSku['class'].astype(str)
        consultaVendasSku['class'].fillna('-', inplace=True)

        consultaVendasSku['classCategoria'] = consultaVendasSku['classCategoria'].astype(str)
        consultaVendasSku['classCategoria'].fillna('-', inplace=True)

        consultaVendasSku['ocorrencia'] = consultaVendasSku.groupby(['marca', 'categoria']).cumcount() + 1
        consultaVendasSku.loc[consultaVendasSku['ocorrencia'] == 1, 'classCategoria'] = 'A1'

        if utilizaCongelento == 'sim':
            consultaVendasSku = consultaVendasSku.loc[:,
                  ['codItemPai', 'class','classCategoria']]

        else:
            consultaVendasSku.drop(['ABCdist%', 'ABCdist%Categoria', "totalVendas", "totalVendasCategoria"], axis=1,
                                   inplace=True)

        return consultaVendasSku



