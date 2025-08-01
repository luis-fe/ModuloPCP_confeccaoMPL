import os

import numpy as np
import pandas as pd
from src.connection import ConexaoPostgre
from src.configApp import configApp
from src.models import Pedidos_CSW, Plano, Produtos, Meta_Plano
import pyarrow.parquet as pq
import pytz
from datetime import datetime


import fastparquet as fp
from dotenv import load_dotenv, dotenv_values

class Pedidos():
    '''Classe que interage junto aos pedidos '''

    def __init__(self, codEmpresa = '1', codPlano = None, consideraPedidosBloqueados = 'sim', codReduzido = ''):

        self.codEmpresa = codEmpresa
        self.codPlano = codPlano
        self.consideraPedidosBloqueados = consideraPedidosBloqueados
        self.codReduzido = codReduzido

    def obtert_tipoNotas(self):
        '''Metodo que obtem todos os tipos de Nota cadastrados'''

        pedidosCsw = Pedidos_CSW.Pedidos_CSW(self.codEmpresa)

        tipoNotas = pedidosCsw.obtendoTipoNotaCsw()

        return tipoNotas
    def pesquisarTipoNotasPlano(self):
        '''Metodo utilizado para obter os tipo de notas de um determinado plano'''

        sql = """
            select
	            "tipo nota" as "codTipoNota"
            from
	            pcp."tipoNotaporPlano" tnp
            where
	            plano = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        return consulta
    def vincularNotasAoPlano(self, arrayTipoNotas):
        ''''Metodo utilizado para vincular Tipo de notas ao Plano '''

        plano = Plano.Plano(self.codPlano)

        # Validando se o Plano ja existe
        validador = plano.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if  validador.empty:

            return pd.DataFrame([{'Status':False,'Mensagem':f'O Plano {self.codPlano} NAO existe'}])
        else:
            delete = """
            DELETE FROM 
                pcp."tipoNotaporPlano" 
            WHERE plano = %s and "tipo nota" = %s and "codEmpresa" = %s
            """

            insert = """INSERT INTO pcp."tipoNotaporPlano" ("tipo nota" , plano, nome, "codEmpresa" ) values 
            ( %s, %s, %s, %s )"""

            conn = ConexaoPostgre.conexaoInsercao()
            cur = conn.cursor()

            for codNota in arrayTipoNotas:

                nomeNota = Pedidos_CSW.Pedidos_CSW(self.codEmpresa, codNota).consultarTipoNotaEspecificoCsw()
                cur.execute(delete, (self.codPlano, str(codNota), self.codEmpresa))
                conn.commit()
                cur.execute(insert,(str(codNota), self.codPlano, nomeNota,self.codEmpresa))
                conn.commit()

            cur.close()
            conn.close()

            return pd.DataFrame([{'Status': True, 'Mensagem': 'TipoNotas adicionados ao Plano com sucesso !'}])


    def listagemPedidosSku(self, detalhaSku = '', iniVendas = '', fimVendas = '', iniFat = '' , fimFat = ''):


        caminho_arquivo = f"{configApp.localArquivoParquet}/pedidos.parquet"

        # Carrega apenas os registros com codProduto == self.codReduzido, se aplicável
        if detalhaSku != '':
            filtro = [('codProduto', '=', self.codReduzido)]
        else:
            filtro = None

        # Apenas as colunas necessárias (exemplo: todas)
        # Para mais performance, especifique colunas como: columns=['codProduto', 'outraColuna']
        tabela = pq.read_table(caminho_arquivo, filters=filtro)

        # Converte para Pandas
        df_loaded = tabela.to_pandas()



        # 3 Obtendo Informacoes do Plano Para filtragem
        plano = Plano.Plano(self.codPlano)

        if self.codPlano != '':
            self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()
            self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()
        else:
            self.iniVendas = iniVendas
            self.fimVendas = fimVendas
            self.iniFat = iniFat
            self.fimFat = fimFat


        #4 Filtrando de acordo com os intervalos encontrados de Vendas e Faturamento
        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)

        df_loaded['filtro'] = df_loaded['dataEmissao'] >= self.iniVendas
        df_loaded['filtro2'] = df_loaded['dataEmissao'] <= self.fimVendas
        df_loaded['filtro3'] = df_loaded['dataPrevFat'] >= self.iniFat
        df_loaded['filtro4'] = df_loaded['dataPrevFat'] <= self.fimFat





        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])

        df_loaded = df_loaded[df_loaded['filtro3'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])

        df_loaded = df_loaded[df_loaded['filtro4'] == True].reset_index(drop=True)

        df_loaded = df_loaded[df_loaded['situacaoPedido'] != '9']

        # 5 expandindo mais informacoes no data Frame: Produtos.Produtos().consultaItensReduzidos()
        produtos = Produtos.Produtos().consultaItensReduzidos()
        produtos.rename(
            columns={'codigo': 'codProduto'},
            inplace=True)
        df_loaded = pd.merge(df_loaded, produtos, on='codProduto', how='left')
        df_loaded['codItemPai'] = df_loaded['codItemPai'].astype(str)
        df_loaded['codItemPai'].fillna('-', inplace=True)



        # 6 - Tratamento de erro nas colunas do data frame
        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = df_loaded['qtdePedida'] - df_loaded['qtdeCancelada']
        df_loaded['valorVendido'] = df_loaded['qtdePedida'] * df_loaded['PrecoLiquido']
        # 6.1 Convertendo para float antes de arredondar
        df_loaded['valorVendido'] = pd.to_numeric(df_loaded['valorVendido'], errors='coerce')
        # 6.2 Aplicando o arredondamento
        df_loaded['valorVendido'] = df_loaded['valorVendido'].round(2)


        # 7 - Filtrando os Tipo de Notas desejados
        tiponotas = self.pesquisarTipoNotasPlano()
        df_loaded = pd.merge(df_loaded, tiponotas, on='codTipoNota')

        if self.consideraPedidosBloqueados == 'nao':
            pedidosBloqueados = self.pedidosBloqueados()
            df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
            df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
            df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']


        # 8 - Incluindo a informacao de Marca no data Frame
        conditions = [
            df_loaded['codItemPai'].str.startswith("102"),
            df_loaded['codItemPai'].str.startswith("202"),
            df_loaded['codItemPai'].str.startswith("104"),
            df_loaded['codItemPai'].str.startswith("204")
        ]
        choices = ["MPOLLO", "MPOLLO", "PACO", "PACO"]
        df_loaded['marca'] = np.select(conditions, choices, default="OUTROS")
        df_loaded = df_loaded[df_loaded['marca'] != 'OUTROS']
        return df_loaded


    def pedidosBloqueados(self):
        '''Metodo que consulta os pedidos Bloqueados'''

        pedidos_CSW = Pedidos_CSW.Pedidos_CSW(self.codEmpresa)
        consulta = pedidos_CSW.pedidosBloqueados()

        return consulta


    def vendasGeraisPorPlano(self):
        '''metodo que carrega as vendas gerais por plano '''

        # 1 - Carregando a lista de produtos vendidos a nivel sku
        df_loaded = self.listagemPedidosSku()
        plano = Plano.Plano(self.codPlano)
        produto = Produtos.Produtos(self.codEmpresa,None,'5')

        # 2 - Agrupando os dados a nivel codReduzido -Marca :
        disponivel = df_loaded.groupby(["codProduto"]).agg({
                                                        "marca":'first',
                                                         "qtdePedida": "sum",
                                                         "qtdeFaturada": 'sum'}).reset_index()
        disponivel.rename(columns={"codProduto":"codReduzido"}, inplace=True)


        # 3 - encontrando o estoque e os produtos em processo
        estoque = produto.estoqueNat()
        emProcesso = produto.emProducao()

        # 4 - Concatenando dados de estoque e emProcesso com o o agrupamento "disponivel"
        disponivel = pd.merge(disponivel, estoque, on='codReduzido', how='left')
        disponivel['estoqueAtual'].fillna(0, inplace=True)
        disponivel = pd.merge(disponivel, emProcesso, on='codReduzido', how='left')
        disponivel['emProcesso'].fillna(0, inplace=True)

        ## 4.1 - Calculando o disponivel e calculando o faltaProgramar em relacao aq que foi Vendido
        disponivel['disponivel'] = (disponivel['emProcesso'] + disponivel['estoqueAtual']) - (
                    disponivel['qtdePedida'] - disponivel['qtdeFaturada'])
        disponivel['faltaProgVendido'] = disponivel['disponivel'].where(disponivel['disponivel'] < 0, 0)

        ### 4.2 - Agrupando por marca o faltaProgVendido
        disponivel = disponivel.groupby(["marca"]).agg({"faltaProgVendido": 'sum'}).reset_index()

        # 5 - Agrupando os dados por Marca
        groupByMarca = df_loaded.groupby(["marca"]).agg({"qtdePedida":"sum","valorVendido":'sum',"qtdeFaturada":"sum"}).reset_index()
        groupByMarca = pd.merge(groupByMarca, disponivel,on='marca', how='left')

        # 6 - Agrupando os dados por Categoria
        groupByCategoria = df_loaded.groupby(["marca","categoria"]).agg({"qtdePedida":"sum","valorVendido":'sum',"qtdeFaturada":"sum"}).reset_index()


        groupByCategoria['qtdePedida2'] = groupByCategoria['qtdePedida']
        groupByCategoria['valorVendido2'] = groupByCategoria['valorVendido']
        groupByCategoria['qtdeFaturada2'] = groupByCategoria['qtdeFaturada']

        groupByCategoria['qtdePedida'] = groupByCategoria['qtdePedida'].apply(self.__formatar_padraoInteiro)
        groupByCategoria['qtdeFaturada'] = groupByCategoria['qtdeFaturada'].apply(self.__formatar_padraoInteiro)
        groupByCategoria['valorVendido'] = groupByCategoria['valorVendido'].apply(self.formatar_financeiro)


        groupByCategoria['marca2'] = groupByCategoria['marca']
        groupByCategoria['marca3'] = groupByCategoria['marca']
        groupByCategoria['marca4'] = groupByCategoria['marca']
        groupByCategoria['marca5'] = groupByCategoria['marca']

        sqlMetaCategoria = """
                select
        			m."nomeCategoria" as "categoria",
        			m."metaPc" ,
        			m."metaFinanceira",
        			m."marca" 
        		from
        			pcp."Meta_Categoria_Plano" m
        		where
        		    m."codPlano" = %s        
                """
        conn = ConexaoPostgre.conexaoEngine()
        sqlMetaCategoria = pd.read_sql(sqlMetaCategoria, conn, params=(self.codPlano,))
        groupByCategoria = pd.merge(groupByCategoria,sqlMetaCategoria,on=['categoria','marca'],how='left')
        groupByCategoria.fillna('-',inplace=True)

        groupByCategoria['metaPc'] = groupByCategoria['metaPc'].apply(self.__formatar_padraoInteiro)
        groupByCategoria['metaFinanceira'] = groupByCategoria['metaFinanceira'].apply(self.formatar_financeiro)


        # Agregar valores por categoria
        groupByCategoria = groupByCategoria.groupby("categoria").agg({
            "marca": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'qtdePedida'])),
            "marca2": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'valorVendido'])),
            "marca3": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'metaPc'])),
            "marca4": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'metaFinanceira'])),
            "marca5": lambda x: dict(zip(x, groupByCategoria.loc[x.index, 'qtdeFaturada'])),
            "qtdePedida2": "sum",
            "valorVendido2":"sum",
            "qtdeFaturada2":'sum'
        }).reset_index()
        #groupByCategoria = groupByCategoria.drop(columns=['marca2'])

        # Renomear colunas, se necessário
        groupByCategoria.rename(columns={"marca": "8.5-qtdVendido",
                                         "marca2":"8.6-valorVendido",
                                         "marca3":"8.7-metaPcs",
                                         "marca5": "8.8-qtdeFaturada",
                                         "marca4": "8.9-metaFinanceira",
                                         "categoria":"8.1-categoria",
                                         "valorVendido2":"8.3-TotalvalorVendido",
                                         "qtdeFaturada2": "8.4-TotalFaturadoPcs",
                                         "qtdePedida2":"8.2-TotalqtdePedida"}, inplace=True)

        totalVendasPeca = groupByMarca['qtdePedida'].sum()
        totalVendasReais = groupByMarca['valorVendido'].sum()
        totalqtdeFaturada = groupByMarca['qtdeFaturada'].sum()
        totalfaltaProgVendido = groupByMarca['faltaProgVendido'].sum()

        groupByCategoria = groupByCategoria.sort_values(by=['8.2-TotalqtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        groupByCategoria['8.5-precoMedioRealizado'] = (groupByCategoria['8.3-TotalvalorVendido'] / groupByCategoria['8.2-TotalqtdePedida']).round(2)

        groupByCategoria['8.2-TotalqtdePedida'] = groupByCategoria['8.2-TotalqtdePedida'].apply(self.__formatar_padraoInteiro)
        groupByCategoria['8.4-TotalFaturadoPcs'] = groupByCategoria['8.4-TotalFaturadoPcs'].apply(self.__formatar_padraoInteiro)

        groupByCategoria['8.3-TotalvalorVendido'] = groupByCategoria['8.3-TotalvalorVendido'].apply(self.formatar_financeiro)
        groupByCategoria['8.5-precoMedioRealizado'] = groupByCategoria['8.5-precoMedioRealizado'].apply(self.formatar_financeiro)



        metas = Meta_Plano.Meta_Plano(self.codEmpresa, self.codPlano)

        metasDataFrame = metas.consultaMetaGeral()
        if metasDataFrame.empty:
            metasDataFrame = pd.DataFrame({
                'marca':['MPOLLO','PACO']
                ,'metaPecas':['0','0']
                , 'metaFinanceira': ['0', '0']

            })
            groupByMarca = pd.merge(groupByMarca,metasDataFrame,on='marca',how='left')
            totalMetasPeca = '0'
            totalMetaFinanceira = 'R$ 0,00'
        else:
            metasDataFrame = metasDataFrame.loc[:,
                        ['marca', 'metaFinanceira', 'metaPecas']]
            groupByMarca = pd.merge(groupByMarca, metasDataFrame, on='marca', how='left')
            totalMetasPeca = metasDataFrame['metaPecas'].str.replace('.','').astype(int).sum()
            # Convertendo para float e somando
            totalMetaFinanceira = metasDataFrame[metasDataFrame['marca']=='TOTAL'].reset_index()
            totalMetaFinanceira = totalMetaFinanceira['metaFinanceira'][0]


        # Convertendo para float antes de arredondar
        groupByMarca['valorVendido'] = pd.to_numeric(groupByMarca['valorVendido'], errors='coerce')
        # Aplicando o arredondamento
        groupByMarca['valorVendido'] = groupByMarca['valorVendido'].round(2)
        groupByMarca['precoMedioRealizado'] = (groupByMarca['valorVendido'] / groupByMarca['qtdePedida']).round(2)

        groupByMarca['precoMedioRealizado'] = groupByMarca['precoMedioRealizado'].apply(self.formatar_financeiro)
        groupByMarca['valorVendido'] = groupByMarca['valorVendido'].apply(self.formatar_financeiro)
        groupByMarca['qtdePedida'] = groupByMarca['qtdePedida'].apply(self.__formatar_padraoInteiro)
        groupByMarca['qtdeFaturada'] = groupByMarca['qtdeFaturada'].apply(self.__formatar_padraoInteiro)
        groupByMarca['qtdeFaturada'].fillna('0',inplace=True)
        groupByMarca['faltaProgVendido'] = groupByMarca['faltaProgVendido'].apply(self.__formatar_padraoInteiro)

        totalPrecoMedio = totalVendasReais/totalVendasPeca

        # Cria a linha de total
        total = pd.DataFrame([{
            'marca': 'TOTAL',
            'metaPecas': f'{totalMetasPeca:,.0f}'.replace(",", "X").replace("X", "."),
            'metaFinanceira': totalMetaFinanceira,
            'qtdePedida':f'{totalVendasPeca:,.0f}'.replace(",", "X").replace("X", "."),
            'faltaProgVendido': f'{totalfaltaProgVendido:,.0f}'.replace(",", "X").replace("X", "."),
            'qtdeFaturada': f'{totalqtdeFaturada:,.0f}'.replace(",", "X").replace("X", "."),
            'valorVendido' : f'R$ {totalVendasReais:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
            'precoMedioRealizado':f'R$ {totalPrecoMedio:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        }])

        # Concatena o total ao DataFrame original
        groupByMarca = pd.concat([groupByMarca, total], ignore_index=True)
        groupByMarca.fillna('-',inplace=True)

        semanaAtual = plano.obterSemanaAtual()
        semanaAtualFat = plano.obterSemanaAtualFat()

        data = {
                '1- Intervalo Venda do Plano:': f'{self.iniVendas} - {self.fimVendas}',
                '2- Semanas de Venda':f'{plano.obterNumeroSemanasVendas()} semanas',
                '3- Semana de Venda Atual':f'{semanaAtual}',
                '4- Intervalo Faturamento do Plano:': f'{self.iniFat} - {self.fimFat}',
                '5- Semanas de Faturamento': f'{plano.obterNumeroSemanasFaturamento()} semanas',
                '6- Semana de Faturamento Atual': f'{semanaAtualFat}',
                '7- Detalhamento:': groupByMarca.to_dict(orient='records'),
                '8- DetalhamentoCategoria': groupByCategoria.to_dict(orient='records')
            }
        return pd.DataFrame([data])


    def __formatar_padraoInteiro(self, valor):
        "metodo que converte valor para formato inteiro int"
        try:
            return f'{valor:,.0f}'.replace(",", "X").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível


    def formatar_financeiro(self,valor):
        "metodo que converte valor para formato financeiro int"

        try:
            return f'R$ {valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível

    def reservaFatAtual(self, detalha = '', diasConsideraPedidos = 30 ):
        '''Metodo que encontra a reserva atual por sku'''
        diaAtual = datetime.strptime(self.__obterDiaAtual(), '%Y-%m-%d')
        plano = Plano.Plano(self.codPlano)
        self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()
        IniFat = datetime.strptime(self.iniFat, '%Y-%m-%d') + pd.Timedelta(days=diasConsideraPedidos)

        print(f'obtendo a reserva saldo dia atual {diaAtual} x inicio fat {IniFat}')
        df_loaded = self.__consultaArquivoFastVendasAnteriores()

        if diaAtual <= IniFat:


            if detalha != '' and detalha != 'todos' :
                disponivel = df_loaded.groupby(["codProduto","codPedido"]).agg({
                    "dataEmissao":"first",
                    "codTipoNota":"first",
                    "dataPrevFat": "first",
                    "qtdePedida": "sum",
                    "qtdeCancelada": "sum",
                    "qtdeFaturada": 'sum'}).reset_index()

                disponivel['dataPrevFat'] = pd.to_datetime(disponivel['dataPrevFat'],
                                                        format='%a, %d %b %Y %H:%M:%S %Z').dt.strftime('%Y-%m-%d')

            elif detalha =='todos':
                disponivel = df_loaded.groupby(["codProduto","codPedido"]).agg({
                    "marca":"first",
                    "nome":"first",
                    "dataEmissao":"first",
                    "codTipoNota":"first",
                    "dataPrevFat": "first",
                    "qtdePedida": "sum",
                    "qtdeCancelada": "sum",
                    "qtdeFaturada": 'sum'}).reset_index()

                disponivel['dataPrevFat'] = pd.to_datetime(disponivel['dataPrevFat'],
                                                        format='%a, %d %b %Y %H:%M:%S %Z').dt.strftime('%Y-%m-%d')



            else:
                disponivel = df_loaded.groupby(["codProduto"]).agg({
                                                                 "qtdePedida": "sum",
                                                                "qtdeCancelada": "sum",
                                                                "qtdeFaturada": 'sum'}).reset_index()
            disponivel.rename(columns={"codProduto":"codReduzido",
                                       "qtdePedida": "qtdePedidaSaldo",
                                       "qtdeCancelada": "qtdeCanceladaSaldo",
                                       "qtdeFaturada": "qtdeFaturadaSaldo"
                                       }, inplace=True)

            disponivel['SaldoColAnt'] = disponivel['qtdePedidaSaldo'] - disponivel['qtdeFaturadaSaldo'] - disponivel['qtdeCanceladaSaldo']



        else:
            disponivel = pd.DataFrame([{'status':'vazio','codReduzido':'',"SaldoColAnt":0}])
        return disponivel




    def vendasPorSku(self):
        '''Metodo que disponibiliza as vendas a nivel de sku do Plano'''

        df_loaded = self.listagemPedidosSku()
        groupBy = df_loaded.groupby(["codProduto"]).agg({"marca":"first",
                                                         "nome":'first',
                                                         "categoria":'first',
                                                         "codCor":"first",
                                                         "codItemPai":'first',
                                                         "qtdePedida":"sum",
                                                         "qtdeFaturada":'sum',
                                                         "valorVendido":'sum',
                                                         "codSeqTamanho":'first',
                                                         "codPedido":'count'}).reset_index()
        groupBy = groupBy.sort_values(by=['qtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        tam = Produtos.Produtos(self.codEmpresa).get_tamanhos()


        groupBy['codSeqTamanho'] = groupBy['codSeqTamanho'].astype(str)
        groupBy['codSeqTamanho'] = groupBy['codSeqTamanho'].astype(str).str.replace('.0', '', regex=False)
        groupBy = pd.merge(groupBy,tam,on='codSeqTamanho',how='left')


        # Renomear colunas, se necessário
        groupBy.rename(columns={"codProduto":"codReduzido","codPedido":"Ocorrencia em Pedidos"}, inplace=True)

        afv = Produtos.Produtos(self.codEmpresa).statusAFV()
        estoque = Produtos.Produtos(self.codEmpresa).estoqueNat()
        emProcesso = Produtos.Produtos(self.codEmpresa).emProducao()

        groupBy = pd.merge(groupBy, afv, on='codReduzido',how='left')
        groupBy['statusAFV'].fillna('Normal',inplace=True)
        groupBy = pd.merge(groupBy, estoque, on='codReduzido',how='left')
        groupBy['estoqueAtual'].fillna(0,inplace=True)
        groupBy = pd.merge(groupBy, emProcesso, on='codReduzido',how='left')
        groupBy['emProcesso'].fillna(0,inplace=True)

        groupBy['disponivel'] = (groupBy['emProcesso'] + groupBy['estoqueAtual'] ) - (groupBy['qtdePedida'] - groupBy['qtdeFaturada'] )
        groupBy['disponivel Pronta Entega'] = (groupBy['estoqueAtual'] ) - (groupBy['qtdePedida'] - groupBy['qtdeFaturada'] )

        groupBy['valorVendido'] = groupBy['valorVendido'].apply(self.formatar_financeiro)
        groupBy['qtdePedida'] = groupBy['qtdePedida'].apply(self.__formatar_padraoInteiro)
        groupBy['qtdeFaturada'] = groupBy['qtdeFaturada'].apply(self.__formatar_padraoInteiro)
        groupBy['disponivel'] = groupBy['disponivel'].apply(self.__formatar_padraoInteiro)
        groupBy['emProcesso'] = groupBy['emProcesso'].apply(self.__formatar_padraoInteiro)
        groupBy['estoqueAtual'] = groupBy['estoqueAtual'].apply(self.__formatar_padraoInteiro)

        return groupBy


    def detalhaPedidosSku(self):
        '''Metodo que consulta os pedidos do sku:
        codPedido, tipoNota, dataEmisao, dataPrev , cliente , qtdPedida
        '''

        df_loaded = self.listagemPedidosSku(self.codReduzido)


        groupBy = df_loaded.groupby(["codPedido"]).agg({"marca":"first",
                                                         "qtdePedida":"sum",
                                                         "qtdeFaturada":'sum',
                                                         "valorVendido":'sum',
                                                        "codTipoNota":"first",
                                                        "dataEmissao":"first",
                                                        "dataPrevFat":"first"}).reset_index()
        groupBy['dataEmissao'] = pd.to_datetime(groupBy['dataEmissao'], format='%a, %d %b %Y %H:%M:%S %Z').dt.strftime('%Y-%m-%d')
        groupBy['dataPrevFat'] = pd.to_datetime(groupBy['dataPrevFat'], format='%a, %d %b %Y %H:%M:%S %Z').dt.strftime('%Y-%m-%d')

        groupBy = groupBy.sort_values(by=['qtdePedida'],
                                                        ascending=False)  # escolher como deseja classificar
        return groupBy



    def detalhaPedidosSkuSaldo(self):
        '''Metodo que consulta os pedidos do sku:
        codPedido, tipoNota, dataEmisao, dataPrev , cliente , qtdPedida
        '''

        df_loaded = self.reservaFatAtual('sim')
        df_loaded = df_loaded[df_loaded['codReduzido'] == self.codReduzido].reset_index()

        df_loaded = df_loaded[df_loaded['SaldoColAnt'] > 0].reset_index()


        df_loaded = df_loaded.sort_values(by=['SaldoColAnt'],
                                                        ascending=False)  # escolher como deseja classificar
        return df_loaded

    def detalhaPedidosGeralaldo(self):
        '''Metodo que consulta os pedidos do sku:
        codPedido, tipoNota, dataEmisao, dataPrev , cliente , qtdPedida
        '''

        df_loaded = self.reservaFatAtual('todos')

        df_loaded = df_loaded[df_loaded['SaldoColAnt'] > 0].reset_index()


        df_loaded = df_loaded.sort_values(by=['SaldoColAnt'],
                                                        ascending=False)  # escolher como deseja classificar
        return df_loaded


    def __consultaArquivoFastVendasAnteriores(self, detalhaSku = ''):
        '''Metodo utilizado para ler um arquivo do tipo parquet e converter em um DataFrame, retornando um DataFrame com as vendas
         nos 300 dias anteriores ao periodo de faturamento do plano atual'''

        caminho_arquivo = f"{configApp.localArquivoParquet}/pedidos.parquet"

        # Carrega apenas os registros com codProduto == self.codReduzido, se aplicável
        if detalhaSku != '':
            filtro = [('codProduto', '=', self.codReduzido)]
        else:
            filtro = None

        # Apenas as colunas necessárias (exemplo: todas)
        # Para mais performance, especifique colunas como: columns=['codProduto', 'outraColuna']
        tabela = pq.read_table(caminho_arquivo, filters=filtro)

        # Converte para Pandas
        df_loaded = tabela.to_pandas()

        # 3 Obtendo Informacoes do Plano Para filtragem
        plano = Plano.Plano(self.codPlano)
        self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()
        self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()

        self.iniFatAnterior = pd.to_datetime(self.iniFat)- pd.Timedelta(days=200)
        # 4 Filtrando de acordo com os intervalos encontrados de Vendas e Faturamento
        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)



        df_loaded['filtro3'] = df_loaded['dataPrevFat'] >= self.iniFatAnterior
        df_loaded['filtro4'] = df_loaded['dataPrevFat'] < self.iniFat



        df_loaded = df_loaded[df_loaded['filtro3'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])

        df_loaded = df_loaded[df_loaded['filtro4'] == True].reset_index(drop=True)

        df_loaded = df_loaded[df_loaded['situacaoPedido'] != '9']

        # 5 expandindo mais informacoes no data Frame: Produtos.Produtos().consultaItensReduzidos()
        produtos = Produtos.Produtos().consultaItensReduzidos()
        produtos.rename(
            columns={'codigo': 'codProduto'},
            inplace=True)
        df_loaded = pd.merge(df_loaded, produtos, on='codProduto', how='left')
        df_loaded['codItemPai'] = df_loaded['codItemPai'].astype(str)
        df_loaded['codItemPai'].fillna('-', inplace=True)

        # 6 - Tratamento de erro nas colunas do data frame
        df_loaded['qtdeSugerida'] = pd.to_numeric(df_loaded['qtdeSugerida'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = pd.to_numeric(df_loaded['qtdePedida'], errors='coerce').fillna(0)
        df_loaded['qtdeFaturada'] = pd.to_numeric(df_loaded['qtdeFaturada'], errors='coerce').fillna(0)
        df_loaded['qtdeCancelada'] = pd.to_numeric(df_loaded['qtdeCancelada'], errors='coerce').fillna(0)
        df_loaded['qtdePedida'] = df_loaded['qtdePedida'] - df_loaded['qtdeCancelada']
        df_loaded['valorVendido'] = df_loaded['qtdePedida'] * df_loaded['PrecoLiquido']
        # 6.1 Convertendo para float antes de arredondar
        df_loaded['valorVendido'] = pd.to_numeric(df_loaded['valorVendido'], errors='coerce')
        # 6.2 Aplicando o arredondamento
        df_loaded['valorVendido'] = df_loaded['valorVendido'].round(2)

        # 7 - Filtrando os Tipo de Notas desejados
        tiponotas = self.pesquisarTipoNotasPlano()
        df_loaded = pd.merge(df_loaded, tiponotas, on='codTipoNota')

        if self.consideraPedidosBloqueados == 'nao':
            pedidosBloqueados = self.pedidosBloqueados()
            df_loaded = pd.merge(df_loaded, pedidosBloqueados, on='codPedido', how='left')
            df_loaded['situacaobloq'].fillna('Liberado', inplace=True)
            df_loaded = df_loaded[df_loaded['situacaobloq'] == 'Liberado']

        # 8 - Incluindo a informacao de Marca no data Frame
        conditions = [
            df_loaded['codItemPai'].str.startswith("102"),
            df_loaded['codItemPai'].str.startswith("202"),
            df_loaded['codItemPai'].str.startswith("104"),
            df_loaded['codItemPai'].str.startswith("204")
        ]
        choices = ["MPOLLO", "MPOLLO", "PACO", "PACO"]
        df_loaded['marca'] = np.select(conditions, choices, default="OUTROS")
        df_loaded = df_loaded[df_loaded['marca'] != 'OUTROS']



        return df_loaded

    def __obterDiaAtual(self):

        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d')
        return agora

