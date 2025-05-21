import os
import fastparquet as fp
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

from src.configApp import configApp
from src.connection import ConexaoPostgre
from src.models import Pedidos, Produtos, Meta_Plano, Lote_Csw, Plano, Tendencia_Plano, Substitutos_Materiais


class Tendencia_Plano_Materiais():
    """Classe que gerencia o processo de Calculo de Tendencia Analise de Materiais de um plano """

    def __init__(self, codEmpresa = '1', codPlano = '', consideraPedBloq = 'nao', codLote='', codComponente='', nomeSimulacao = 'nao'):
        '''Contrutor da classe '''
        self.codEmpresa = codEmpresa
        self.codPlano = codPlano
        self.consideraPedBloq = consideraPedBloq
        self.codLote = codLote
        self.nomeSimulacao = nomeSimulacao
        self.codComponente = codComponente



    def estruturaItens(self, pesquisaPor = 'lote', arraySimulaAbc = 'nao', simula = 'nao'):
        produtos = Produtos.Produtos(self.codEmpresa)

        if pesquisaPor == 'lote':
            consumo = Lote_Csw.Lote_Csw(self.codLote, self.codEmpresa)
            consumo = consumo.get_materiaPrima_loteProd_CSW()



        else:
            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

            inPesquisa = self.__estruturaPrevisao()
            print(f'1 - Leitura da pesquisa dos skus a serem buscados {agora} ')
            if simula == 'nao':
                sqlMetas = Tendencia_Plano.Tendencia_Plano(self.codEmpresa, self.codPlano, self.consideraPedBloq).tendenciaVendas('nao')
            else:
                sqlMetas = Tendencia_Plano.Tendencia_Plano(self.codEmpresa, self.codPlano, self.consideraPedBloq).simulacaoPeloNome()

            consumo = produtos.carregandoComponentes()
            consumo = pd.merge(consumo, inPesquisa, on='codEngenharia')

            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            print(f'2 -Carregando os componentes utilizando nas engenharias {agora} ')



            sqlEstoque = produtos.estMateriaPrima()
            # Agrupando as requisicoes compromedito pelo CodComponente
            sqlEstoque = sqlEstoque.groupby(["CodComponente"]).agg(
                {"estoqueAtual": "sum"}).reset_index()

            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            print(f'3 -Sql do estoque de Materia Prima  {agora} ')

            # Carregando as requisicoes em aberto
            sqlRequisicaoAberto = produtos.req_Materiais_aberto()
            # Congelando o dataFrame de Requisicoes em aberto
            caminho_absoluto2 = configApp.localProjeto
            sqlRequisicaoAberto.to_csv(f'{caminho_absoluto2}/dados/requisicoesEmAberto.csv')

            # Agrupando as requisicoes compromedito pelo CodComponente
            sqlRequisicaoAberto = sqlRequisicaoAberto.groupby(["CodComponente"]).agg(
                {"EmRequisicao": "sum"}).reset_index()

            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            print(f'4 -Sql requisicoes em aberto {agora} ')

            sqlAtendidoParcial = produtos.req_atendidoComprasParcial()
            sqlPedidos = produtos.pedidoComprasMP()
            sqlPedidos = pd.merge(sqlPedidos, sqlAtendidoParcial, on=['numero', 'seqitem'], how='left')

            sqlPedidos['qtAtendida'].fillna(0, inplace=True)

            # Realizando o tratamento do fator de conversao de compras dos componentes
            sqlPedidos['fatCon2'] = sqlPedidos['fatCon'].apply(self.__process_fator)
            sqlPedidos['qtdPedida'] = sqlPedidos['fatCon2'] * sqlPedidos['qtdPedida']

            sqlPedidos['SaldoPedCompras'] = sqlPedidos['qtdPedida'] - sqlPedidos['qtAtendida']

            # Congelando o dataFrame de Pedidos em aberto
            sqlPedidos.to_csv(f'{caminho_absoluto2}/dados/pedidosEmAberto.csv')

            sqlPedidos = sqlPedidos.groupby(["CodComponente"]).agg(
                {"SaldoPedCompras": "sum"}).reset_index()

            sqlMetas['codSortimento'] = sqlMetas['codSortimento'].astype(str)
            sqlMetas['codSortimento'] = sqlMetas['codSortimento'].astype(float).astype(int).astype(str)

            sqlMetas['codSeqTamanho'] = sqlMetas['codSeqTamanho'].astype(str)

            Necessidade = pd.merge(sqlMetas, consumo, on=["codItemPai", "codSeqTamanho", "codSortimento"], how='left')

            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            print(f'6 -Merge das metas x consumo {agora} ')

            Necessidade['categoriaMP'] = '-'
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('ZIPER', row['descricaoComponente'], 'ZIPER', row['categoriaMP']), axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('ENTRETELA', row['descricaoComponente'], 'ENTRETELA', row['categoriaMP']),
                axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('RIBANA', row['descricaoComponente'], 'RIBANA', row['categoriaMP']), axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('ENTRETELA', row['descricaoComponente'], 'ENTRETELA', row['categoriaMP']),
                axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('GOLA', row['descricaoComponente'], 'GOLAS', row['categoriaMP']), axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('KIT GOLA', row['descricaoComponente'], 'KIT GOLA/PUNHO',
                                             row['categoriaMP']),
                axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('MALHA', row['descricaoComponente'], 'MALHA', row['categoriaMP']), axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('TECIDO', row['descricaoComponente'], 'TECIDO PLANO', row['categoriaMP']),
                axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('ETIQUETA', row['descricaoComponente'], 'ETIQUETAS', row['categoriaMP']), axis=1)

            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('CADAR', row['descricaoComponente'], 'CADARCO', row['categoriaMP']), axis=1)
            Necessidade['categoriaMP'] = Necessidade.apply(
                lambda row: self.__categoria('ELAST', row['descricaoComponente'], 'ELASTICOS', row['categoriaMP']), axis=1)

            # Salvar o DataFrame na memoria:

            # Verificar se é para congelar a simulacao
            if simula == 'nao':
                Necessidade.to_csv(f'{caminho_absoluto2}/dados/NecessidadePrevisao{self.codPlano}.csv')
            else:
                Necessidade.to_csv(
                    f'{caminho_absoluto2}/dados/NecessidadePrevisao{self.codPlano}_{self.nomeSimulacao}.csv')

            Necessidade['faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)'] * Necessidade['quantidade']

            Necessidade['disponivelVendas'] = Necessidade['disponivel'] * Necessidade['quantidade']

            Necessidade = Necessidade.groupby(["CodComponente"]).agg(
                {"disponivelVendas": "sum",
                 "faltaProg (Tendencia)": "sum",
                 "descricaoComponente": 'first',
                 "unid": 'first'
                 }).reset_index()
            Necessidade = pd.merge(Necessidade, sqlPedidos, on='CodComponente', how='left')
            Necessidade = pd.merge(Necessidade, sqlRequisicaoAberto, on='CodComponente', how='left')
            Necessidade = pd.merge(Necessidade, sqlEstoque, on='CodComponente', how='left')



            Necessidade['SaldoPedCompras'].fillna(0, inplace=True)
            Necessidade['EmRequisicao'].fillna(0, inplace=True)
            Necessidade['estoqueAtual'].fillna(0, inplace=True)
            Necessidade['Necessidade faltaProg (Tendencia)'] = (Necessidade['faltaProg (Tendencia)']) + Necessidade[
                'estoqueAtual'] + Necessidade['SaldoPedCompras'] - Necessidade['EmRequisicao']
            # -0 + 1.747 + 2 -741,49 ( o negativo significa necessidade de compra)
            Necessidade['saldo Novo'] = Necessidade['Necessidade faltaProg (Tendencia)'].where(
                Necessidade['Necessidade faltaProg (Tendencia)'] > 0, 0)
            Necessidade['saldo Novo'] = Necessidade['saldo Novo'] - Necessidade['SaldoPedCompras']

            # Consulta o substitutos:
            obterSubstitutos = Substitutos_Materiais.Substituto().consultaSubstitutos()
            obterSubstitutos.rename(
                columns={'codMateriaPrimaSubstituto': 'codEditado'},
                inplace=True)
            informacoes = produtos.informacoesComponente()
            Necessidade = pd.merge(Necessidade, informacoes, on='CodComponente', how='left')

            NecessidadeSubstituto = Necessidade.groupby('codEditado').agg({'saldo Novo': 'sum'}).reset_index()

            obterSubstitutos = pd.merge(obterSubstitutos, NecessidadeSubstituto, on='codEditado', how='left')
            obterSubstitutos.fillna(0, inplace=True)
            obterSubstitutos.rename(
                columns={'saldo Novo': 'Saldo Substituto', 'codEditado': 'codMateriaPrimaSubstituto',
                         'codMateriaPrima': 'codEditado'},
                inplace=True)

            Necessidade = pd.merge(Necessidade, obterSubstitutos, on='codEditado', how='left')
            Necessidade['Saldo Substituto'].fillna(0, inplace=True)
            obterSubstitutos.fillna('-', inplace=True)

            Necessidade['Saldo Substituto'] = Necessidade['Saldo Substituto'].where(Necessidade['Saldo Substituto'] > 0,
                                                                                    0)

            Necessidade['Necessidade faltaProg (Tendencia)'] = Necessidade['Necessidade faltaProg (Tendencia)'] + \
                                                               Necessidade['Saldo Substituto']
            Necessidade['Necessidade faltaProg (Tendencia)'] = Necessidade['Necessidade faltaProg (Tendencia)'].where(
                Necessidade['Necessidade faltaProg (Tendencia)'] < 0, 0)

            Necessidade['estoqueAtual'] = Necessidade['estoqueAtual'].apply(self.__formatar_float)
            Necessidade['EmRequisicao'] = Necessidade['EmRequisicao'].apply(self.__formatar_float)
            Necessidade['SaldoPedCompras'] = Necessidade['SaldoPedCompras'].apply(self.__formatar_float)

            Necessidade['loteMut'].fillna(1, inplace=True)
            Necessidade['LoteMin'].fillna(0, inplace=True)

            Necessidade['LeadTime'] = Necessidade['LeadTime'].apply(self.__formatar_padraoInteiro)

            Necessidade.fillna('-', inplace=True)
            Necessidade.rename(
                columns={'CodComponente': '01-codReduzido',
                         'codEditado': '02-codCompleto',
                         'descricaoComponente': '03-descricaoComponente',
                         'fornencedorPreferencial': '04-fornencedorPreferencial',
                         'unid': '05-unidade',
                         'faltaProg (Tendencia)': '06-Necessidade faltaProg(Tendencia)',
                         'EmRequisicao': '07-EmRequisicao',
                         'estoqueAtual': '08-estoqueAtual',
                         'SaldoPedCompras': '09-SaldoPedCompras',
                         'Necessidade faltaProg (Tendencia)': '10-Necessidade Compra (Tendencia)',
                         'LeadTime': '13-LeadTime',
                         'LoteMin': '14-Lote Mínimo',
                         'codMateriaPrimaSubstituto': '15-CodSubstituto',
                         'nomeCodSubstituto': '16-NomeSubstituto',
                         'Saldo Substituto': '17-SaldoSubs',
                         'loteMut': '11-Lote Mutiplo'
                         },
                inplace=True)

            Necessidade = Necessidade.drop(columns=['nomeCodMateriaPrima', 'novoNome', 'saldo Novo'])

            # Encontrando o saldo restante

            # Função para ajustar a necessidade
            def ajustar_necessidade(necessidade, lote_multiplo, lotemin):
                necessidade = necessidade * -1
                if necessidade > 0 and necessidade < lotemin:

                    return lotemin
                else:
                    if lote_multiplo != 0:

                        return np.ceil(necessidade / lote_multiplo) * lote_multiplo
                    else:
                        return necessidade

            # Aplicando o ajuste
            Necessidade["12-Necessidade Ajustada Compra (Tendencia)"] = Necessidade.apply(
                lambda row: ajustar_necessidade(row["10-Necessidade Compra (Tendencia)"], row["11-Lote Mutiplo"],
                                                row["14-Lote Mínimo"]), axis=1
            )

            Necessidade = Necessidade.drop(columns=['disponivelVendas'])
            Necessidade['12-Necessidade Ajustada Compra (Tendencia)'] = Necessidade[
                '12-Necessidade Ajustada Compra (Tendencia)'].apply(self.__formatar_float)
            Necessidade['10-Necessidade Compra (Tendencia)'] = Necessidade['10-Necessidade Compra (Tendencia)'] * -1
            Necessidade['11-Lote Mutiplo'] = Necessidade['11-Lote Mutiplo'].apply(self.__formatar_float)
            Necessidade['10-Necessidade Compra (Tendencia)'] = Necessidade['10-Necessidade Compra (Tendencia)'].apply(
                self.__formatar_float)
            Necessidade['14-Lote Mínimo'] = Necessidade['14-Lote Mínimo'].apply(self.__formatar_float)
            Necessidade['06-Necessidade faltaProg(Tendencia)'] = Necessidade[
                '06-Necessidade faltaProg(Tendencia)'].apply(self.__formatar_float)
            Necessidade = Necessidade[Necessidade['02-codCompleto'] != '-']
            Necessidade = Necessidade[Necessidade['02-codCompleto'] != '-']

            Necessidade = Necessidade.drop_duplicates()

            return Necessidade

    def __estruturaPrevisao(self):

        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        caminho_absoluto = configApp.localArquivoParquet

        # 1.2 - Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminho_absoluto}/pedidos.parquet')

        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()
        plano = Plano.Plano(self.codPlano)
        self.iniVendas, self.fimVendas = plano.pesquisarInicioFimVendas()
        self.iniFat, self.fimFat = plano.pesquisarInicioFimFat()
        produtos = Produtos.Produtos(self.codEmpresa).consultaItensReduzidos()
        produtos.rename(
            columns={'codigo': 'codProduto'},
            inplace=True)
        df_loaded['dataEmissao'] = pd.to_datetime(df_loaded['dataEmissao'], errors='coerce', infer_datetime_format=True)
        df_loaded['dataPrevFat'] = pd.to_datetime(df_loaded['dataPrevFat'], errors='coerce', infer_datetime_format=True)
        df_loaded['filtro'] = df_loaded['dataEmissao'] >= self.iniVendas
        df_loaded['filtro2'] = df_loaded['dataEmissao'] <= self.fimVendas
        df_loaded['filtro3'] = df_loaded['dataPrevFat'] >= self.iniFat
        df_loaded['filtro4'] = df_loaded['dataPrevFat'] <= self.fimFat
        df_loaded = df_loaded[df_loaded['filtro'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['filtro2'] == True].reset_index()
        # print(df_loaded['filtro3'].drop_duplicates())
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro3'] == True].reset_index()
        if 'level_0' in df_loaded.columns:
            df_loaded = df_loaded.drop(columns=['level_0'])
        df_loaded = df_loaded[df_loaded['filtro4'] == True].reset_index()
        df_loaded = df_loaded[df_loaded['situacaoPedido'] != '9']
        df_loaded['codProduto'] = df_loaded['codProduto'].astype(str)

        df_loaded = df_loaded.loc[:,
                         ['codProduto']]

        produtos['codItemPai'] = produtos['codItemPai'].astype(str)


        df_loaded = pd.merge(df_loaded, produtos, on='codProduto', how='left')

        df_loaded['codItemPai'].fillna('-', inplace=True)
        df_loaded = df_loaded.loc[:,
                         ['codItemPai']]
        df_loaded = df_loaded.drop_duplicates(subset='codItemPai')




        df_loaded['codEngenharia'] = '0'+df_loaded['codItemPai']+'-0'
        df_loaded['codItemPai'].fillna('-',inplace=True)


        #result = f"({', '.join(repr(x) for x in df_loaded['codItemPai'])})"


        return df_loaded

    def __formatar_float(self, valor):
        try:
            return f'{valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível


    def __process_fator(self, value):
        if ";*" in value or value.startswith("*;"):  # Caso "*;N", remover prefixo
            num = int(value.replace("*;", "").replace(";*", ""))
            return num / 1000
        elif value.startswith("*"):  # Caso "*N", multiplicar o número por 1000
            num = int(value.replace("*", ""))
            return 1
        else:  # Caso padrão, converter direto
            return 1


    def __formatar_padraoInteiro(self, valor):
        try:
            return f'{valor:,.0f}'.replace(",", "X").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível


    def detalhaNecessidade(self, simulacao):
        '''metodo que detalha a necessidade de um componente '''

        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        caminho_absoluto = configApp.localProjeto


        if simulacao == 'nao':
            Necessidade = pd.read_csv(f'{caminho_absoluto}/dados/NecessidadePrevisao{self.codPlano}.csv')
        else:
            Necessidade = pd.read_csv(f'{caminho_absoluto}/dados/NecessidadePrevisao{self.codPlano}_{self.nomeSimulacao}.csv')

        Necessidade['CodComponente'] = Necessidade['CodComponente'].astype(str)
        Necessidade['CodComponente'] = Necessidade['CodComponente'].str.replace('.0','')
        Necessidade = Necessidade[Necessidade['CodComponente']==self.codComponente].reset_index()
        Necessidade['Necessidade faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)'] * (Necessidade['quantidade'] * -1)


        Necessidade.rename(
            columns={'codEngenharia': '01-codEngenharia',
                     'codReduzido': '02-codReduzido',
                     'nome': '03-nome',
                     'tam': '04-tam',
                     'codCor': '05-codCor',
                     'qtdePedida': '06-qtdePedida',
                     'Ocorrencia em Pedidos':'07-Ocorrencia em Pedidos',
                     'statusAFV': '08-statusAFV',
                     'previcaoVendas': '09-previcaoVendas',
                     'faltaProg (Tendencia)': '10-faltaProg (Tendencia)',
                     'CodComponente': '11-CodComponente',
                     'unid': '12-unid',
                     'quantidade':'13-consumoUnit',
                     'Necessidade faltaProg (Tendencia)':'14-Necessidade faltaProg (Tendencia)'
                     },
            inplace=True)
        Necessidade = Necessidade.drop(columns=['Prev Sobra','Unnamed: 0','categoria','marca','index','descricaoComponente'
            ,'descricaoPlano','disponivel','dist%','emProcesso','estoqueAtual','codItemPai','codPlano','codSeqTamanho','codSortimento',
                                                'valorVendido','qtdeFaturada'])

        Necessidade['14-Necessidade faltaProg (Tendencia)'] = Necessidade['14-Necessidade faltaProg (Tendencia)'].round(2)

        return Necessidade


    def calculoIdealPcs_para_materiaPrima(self, simulacao = 'nao', arrayFiltroCategoria = ''):
        '''Metodo que calcula o numero de peças necessario para atender a materia prima baseado no falta programar'''
        caminho_absoluto2 = configApp.localProjeto

        if simulacao == 'nao':
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/NecessidadePrevisao{self.codPlano}.csv')
        else:
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/NecessidadePrevisao{self.codPlano}.csv')

        Necessidade['faltaProg (Tendencia)MP'] = Necessidade['faltaProg (Tendencia)'] * Necessidade['quantidade']

        Necessidade['disponivelVendasMP'] = Necessidade['disponivel'] * Necessidade['quantidade']


        produtos = Produtos.Produtos(self.codEmpresa)

        sqlEstoque = produtos.estMateriaPrima()
        # Agrupando as requisicoes compromedito pelo CodComponente
        sqlEstoque = sqlEstoque.groupby(["CodComponente"]).agg(
            {"estoqueAtual": "sum"}).reset_index()

        Necessidade['CodComponente'] = Necessidade['CodComponente'].apply(
            lambda x: str(int(float(x))) if pd.notnull(x) else x
        )

        sqlEstoque.rename(
            columns={'estoqueAtual': 'estoqueAtualMP'},
            inplace=True)
        Necessidade = pd.merge(Necessidade, sqlEstoque, on='CodComponente', how='left')
        Necessidade['estoqueAtualMP'].fillna(0,inplace=True)
        Necessidade['faltaProg (Tendencia)MP_total'] = (
            Necessidade.groupby('CodComponente')['faltaProg (Tendencia)MP']
            .transform('sum')
            .round(3)  # duas casas decimais
        )

        Necessidade['Diferenca'] = Necessidade['estoqueAtualMP'] - (-1*Necessidade['faltaProg (Tendencia)MP_total'])
        Necessidade['distrEstoqueMP'] = (
                Necessidade['faltaProg (Tendencia)MP'] / Necessidade['faltaProg (Tendencia)MP_total']
        ).round(4)
        Necessidade['EstoqueDistMP'] =  (Necessidade['distrEstoqueMP'] *  Necessidade['estoqueAtualMP']).round(3)

        Necessidade['Sugestao_PCs'] =  (Necessidade['EstoqueDistMP'] /Necessidade['quantidade']).round(0)

        Necessidade['Sugestao_PCs'] = np.where(
            Necessidade['Diferenca']<0,
            (Necessidade['EstoqueDistMP'] / Necessidade['quantidade']).round(0),
            (Necessidade['faltaProg (Tendencia)'] * -1)
        )

        Necessidade.to_csv(f'{caminho_absoluto2}/dados/MeuTeste2.csv')

        if arrayFiltroCategoria == '':
            Necessidade = Necessidade
        else:
            # Transformar o arryau em dataFrame e fazer o merge
            Necessidade = Necessidade
        print(arrayFiltroCategoria)


        Necessidade = Necessidade.groupby(["codReduzido"]).agg(
                {
                    "marca":"first",
                    "codEngenharia":"first",
                    "nome":"first",
                    "categoria":"first",
                    "codCor":"first",
                    "tam":"first",
                    "faltaProg (Tendencia)":"first",
                    "Sugestao_PCs": "min"}).reset_index()

        return Necessidade



    def __categoria(self, contem, valorReferencia, valorNovo, categoria):
        if contem in valorReferencia and categoria == '-':
            return valorNovo
        else:
            return categoria


    def obter_categoriasMP(self):
        '''Metodo utilizado para filtro de categoria de MP'''

        dados = {
            "categoriaMP": [
                "-",
                "CADARCO",
                "ELASTICOS",
                "ENTRETELA",
                "ETIQUETAS",
                "GOLAS",
                "MALHA",
                "RIBANA",
                "TECIDO PLANO",
                "ZIPER"
            ]
        }

        df = pd.DataFrame(dados)

        return df