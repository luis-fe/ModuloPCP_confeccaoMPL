import os
import fastparquet as fp
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import pytz

from src.configApp import configApp
from src.connection import ConexaoPostgre
from src.models import Pedidos, Produtos, Meta_Plano, Lote_Csw, Plano, Tendencia_Plano, Substitutos_Materiais


class Tendencia_Plano_Materiais():
    """Classe que gerencia o processo de Calculo de Tendencia Analise de Materiais de um plano """

    def __init__(self, codEmpresa = '1', codPlano = '', consideraPedBloq = 'nao', codLote='', codComponente='', nomeSimulacao = 'nao', codReduzido =''):
        '''Contrutor da classe '''
        self.codEmpresa = codEmpresa
        self.codPlano = codPlano
        self.consideraPedBloq = consideraPedBloq
        self.codLote = codLote
        self.nomeSimulacao = nomeSimulacao
        self.codComponente = codComponente
        self.codReduzido = str(codReduzido)


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
            Necessidade['categoriaMP'] = Necessidade.apply(self.aplicar_categorias, axis=1)


            # Salvar o DataFrame na memoria:
            informacoes = produtos.informacoesComponente()
            Necessidade = pd.merge(Necessidade, informacoes, on='CodComponente', how='left')

            # Verificar se é para congelar a simulacao
            if simula == 'nao':
                Necessidade.to_csv(f'{caminho_absoluto2}/dados/EstruturacaoPrevisao{self.codPlano}.csv')
            else:
                Necessidade.to_csv(
                    f'{caminho_absoluto2}/dados/EstruturacaoPrevisao{self.codPlano}_{self.nomeSimulacao}.csv')

            Necessidade['faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)'] * Necessidade['quantidade']

            Necessidade['disponivelVendas'] = Necessidade['disponivel'] * Necessidade['quantidade']

            Necessidade = Necessidade.groupby(["CodComponente"]).agg(
                {"codEditado":"first",
                 "fatorConversao":"first",
                 "fornencedorPreferencial":"first",
                 "novoNome":"first",
                 "LeadTime":"first",
                 "loteMut":"first",
                 "LoteMin":"first",
                    "disponivelVendas": "sum",
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

            self.atualizando_InserindoCalAnalise()

            Necessidade.to_csv(f'{caminho_absoluto2}/dados/NecessidadePrevisaoCongelada{self.codPlano}.csv')

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


        if simulacao == 'nao' or simulacao == '' :
            print(f'simulacao{simulacao}')
            Necessidade = pd.read_csv(f'{caminho_absoluto}/dados/EstruturacaoPrevisao{self.codPlano}.csv')
        else:
            self.nomeSimulacao = simulacao
            Necessidade = pd.read_csv(f'{caminho_absoluto}/dados/EstruturacaoPrevisao{self.codPlano}_Simulacao{self.nomeSimulacao}.csv')
            Necessidade['faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)Simulacao']


        Necessidade['CodComponente'] = Necessidade['CodComponente'].astype(float).astype(int).astype(str)

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
        Necessidade = Necessidade.drop(columns=['Prev Sobra','Unnamed: 0','categoria','marca','index'
            ,'descricaoPlano','disponivel','dist%','emProcesso','estoqueAtual','codItemPai','codPlano','codSeqTamanho','codSortimento',
                                                'valorVendido','qtdeFaturada'])

        Necessidade['14-Necessidade faltaProg (Tendencia)'] = Necessidade['14-Necessidade faltaProg (Tendencia)'].round(2)
        Necessidade['11-CodComponente'] = Necessidade['11-CodComponente'] +'-'+Necessidade['descricaoComponente']+' ('+Necessidade['12-unid']+f')  |{self.nomeSimulacao}'

        return Necessidade


    def calculoIdealPcs_para_materiaPrima(self, simulacao = 'nao', arrayFiltroCategoria = ''):
        '''Metodo que calcula o numero de peças necessario para atender a materia prima baseado no falta programar'''
        caminho_absoluto2 = configApp.localProjeto

        if simulacao == 'nao' or simulacao == '':
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/EstruturacaoPrevisao{self.codPlano}.csv')
        else:
            verificador = self.obtendoUltimaAnalise_porMPPlanoSimulacao()
            if  verificador['status'][0] == False:
                self.estrutura_ItensCongelada('sim')

            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/EstruturacaoPrevisao{self.codPlano}_Simulacao{self.nomeSimulacao}.csv')
            Necessidade['faltaProg (Tendencia)']= Necessidade['faltaProg (Tendencia)Simulacao']

        Necessidade['faltaProg (Tendencia)MP'] = Necessidade['faltaProg (Tendencia)'] * Necessidade['quantidade']

        Necessidade['disponivelVendasMP'] = Necessidade['disponivel'] * Necessidade['quantidade']


        produtos = Produtos.Produtos(self.codEmpresa)

        # carregando os dados do Estoque de Matéria Prima
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

        # Carregando as requisicoes em aberto
        sqlRequisicaoAberto = produtos.req_Materiais_aberto()
        # Congelando o dataFrame de Requisicoes em aberto
        caminho_absoluto2 = configApp.localProjeto

        # Agrupando as requisicoes compromedito pelo CodComponente
        sqlRequisicaoAberto = sqlRequisicaoAberto.groupby(["CodComponente"]).agg(
            {"EmRequisicao": "sum"}).reset_index()

        Necessidade = pd.merge(Necessidade, sqlRequisicaoAberto, on='CodComponente', how='left')
        Necessidade['EmRequisicao'].fillna(0,inplace=True)
        Necessidade['EmRequisicao'] = Necessidade['EmRequisicao'].round(2)


        Necessidade['EstoqueAtualMPLiquido'] = Necessidade['estoqueAtualMP'] - Necessidade['EmRequisicao']
        Necessidade['EstoqueAtualMPLiquido'] = Necessidade['EstoqueAtualMPLiquido'].round(2)


        Necessidade['faltaProg (Tendencia)MP_total'] = (
            Necessidade.groupby('CodComponente')['faltaProg (Tendencia)MP']
            .transform('sum')
            .round(3)  # duas casas decimais
        )

        Necessidade['Diferenca'] = Necessidade['EstoqueAtualMPLiquido'] - (-1*Necessidade['faltaProg (Tendencia)MP_total'])
        Necessidade['distrEstoqueMP'] = (
                Necessidade['faltaProg (Tendencia)MP'] / Necessidade['faltaProg (Tendencia)MP_total']
        ).round(4)
        Necessidade['EstoqueDistMP'] =  (Necessidade['distrEstoqueMP'] *  Necessidade['EstoqueAtualMPLiquido']).round(3)

        Necessidade['Sugestao_PCs'] =  (Necessidade['EstoqueDistMP'] /Necessidade['quantidade']).round(0)

        Necessidade['Sugestao_PCs'] = np.where(
            Necessidade['Diferenca']<0,
            (Necessidade['EstoqueDistMP'] / Necessidade['quantidade']).round(0),
            (Necessidade['faltaProg (Tendencia)'] * -1)
        )

        Necessidade['faltaProg (Tendencia)MP_total'] = Necessidade['faltaProg (Tendencia)MP_total']   * -1
        Necessidade['faltaProg (Tendencia)'] = Necessidade['faltaProg (Tendencia)']   * -1


        if self.nomeSimulacao == 'nao' or self.nomeSimulacao =='':
            Necessidade.to_csv(f'{caminho_absoluto2}/dados/DetalhamentoGeralProgramacao{self.codPlano}.csv')
        else:

            self.atualizando_MPPlanoSimulacao()
            Necessidade.to_csv(
                f'{caminho_absoluto2}/dados/DetalhamentoGeralProgramacao{self.codPlano}{self.nomeSimulacao}.csv')

        if arrayFiltroCategoria == [] or arrayFiltroCategoria == '' :
            Necessidade = Necessidade
        else:
            # Transformar o arryau em dataFrame e fazer o merge
            arrayFiltroCategoria =  pd.DataFrame(arrayFiltroCategoria, columns=['categoriaMP'])
            Necessidade = pd.merge(Necessidade,arrayFiltroCategoria,on='categoriaMP')
        print(arrayFiltroCategoria)


        Necessidade = Necessidade.groupby(["codReduzido"]).agg(
                {
                    "marca":"first",
                    "codEngenharia":"first",
                    "nome":"first",
                    "categoria":"first",
                    "codCor":"first",
                    "tam":"first",
                    "disponivel":"first",
                    "faltaProg (Tendencia)":"first",
                    "Sugestao_PCs": "min"}).reset_index()

        Necessidade['Sugestao_PCs'] = np.where(
            Necessidade['Sugestao_PCs']>0,
            Necessidade['Sugestao_PCs'],
            0
        )


        return Necessidade



    def __categoria(self, contem, valorReferencia, valorNovo, categoria):
        if isinstance(valorReferencia, str) and contem in valorReferencia and categoria == '-':
            return valorNovo
        else:
            return categoria


    def obter_categoriasMP(self):
        '''Metodo utilizado para filtro de categoria de MP'''

        dados = {
            "categoriaMP": [
                "-",
                "PRODUTO REVENDA",
                "TAGS/TRAVANEL",
                "ITENS CAMISARIA",
                "CADARCO/CORDAO",
                "ELASTICOS",
                "ENTRETELA",
                "ETIQUETAS",
                "GOLAS",
                "MALHA",
                "MOLETOM"
                "RIBANA",
                "TECIDO PLANO",
                "ZIPER"
            ]
        }

        df = pd.DataFrame(dados)

        return df


    def detalharSku_x_AnaliseEmpenho(self, simulacao = 'nao',arrayFiltroCategoria = ''):
        '''Metodo que detalha a analise de emprenho filtrado x Sku selecionando  '''

        caminho_absoluto2 = configApp.localProjeto

        if simulacao == 'nao' or simulacao == '':
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/DetalhamentoGeralProgramacao{self.codPlano}.csv')
        else:
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/DetalhamentoGeralProgramacao{self.codPlano}{self.nomeSimulacao}.csv')


        if arrayFiltroCategoria == [] or arrayFiltroCategoria == '' :
            Necessidade['obs'] = 'Restringe'
        else:
            # Transformar o arryau em dataFrame e fazer o merge
            arrayFiltroCategoria =  pd.DataFrame(arrayFiltroCategoria, columns=['categoriaMP'])
            arrayFiltroCategoria['obs'] = 'Restringe'
            Necessidade = pd.merge(Necessidade,arrayFiltroCategoria,on='categoriaMP', how='left')
            Necessidade['obs'].fillna('nao restringe',inplace=True)

        Necessidade['codReduzido'] = Necessidade['codReduzido'].astype(str)
        Necessidade =  Necessidade[Necessidade['codReduzido'] == self.codReduzido].reset_index()
        if simulacao == 'nao' or simulacao == '':
            Necessidade = Necessidade.groupby(["codReduzido","CodComponente"]).agg(
                    {
                        "codEditado":"first",
                        "codItemPai": "first",
                        "nome": "first",
                        "estoqueAtualMP":"first",
                        "EmRequisicao": "first",
                        "EstoqueAtualMPLiquido": "first",
                        "faltaProg (Tendencia)MP_total": "first",
                        "descricaoComponente":"first",
                        "EstoqueDistMP": "first",
                        "faltaProg (Tendencia)":"first",
                        "obs":"first",
                        "Sugestao_PCs": "first"}).reset_index()
        else:
            Necessidade = Necessidade.groupby(["codReduzido","CodComponente"]).agg(
                    {
                        "codEditado": "first",
                        "codItemPai":"first",
                        "nome": "first",
                        "estoqueAtualMP":"first",
                        "EmRequisicao": "first",
                        "EstoqueAtualMPLiquido": "first",
                        "faltaProg (Tendencia)MP_total": "first",
                        "descricaoComponente":"first",
                        "EstoqueDistMP": "first",
                        "faltaProg (Tendencia)Simulacao":"first",
                        "obs":"first",
                        "Sugestao_PCs": "first"}).reset_index()

            Necessidade.rename(
                columns={
                    'faltaProg (Tendencia)Simulacao': 'faltaProg (Tendencia)'
                },
                inplace=True)



        Necessidade['Sugestao_PCs'] = np.where(
            Necessidade['Sugestao_PCs']>0,
            Necessidade['Sugestao_PCs'],
            0
        )

        return Necessidade

    def aplicar_categorias(self, row):
        categorias = [
            ('ZIPER', 'ZIPER'),
            ('MALHA CONFORTO ', 'MALHA'),
            ('BONE', 'PRODUTO REVENDA'),
            ('CARTEIRA', 'PRODUTO REVENDA'),
            ('KIT TAG', 'TAGS/TRAVANEL'),
            ('TRAVANEL', 'TAGS/TRAVANEL'),
            ('BORBOLETA', 'ITENS CAMISARIA'),
            ('PRENDEDOR', 'ITENS CAMISARIA'),
            ('SUPORTE', 'ITENS CAMISARIA'),
            ('ENTRETELA', 'ENTRETELA'),
            ('RIBANA', 'RIBANA'),
            ('GOLA', 'GOLAS'),
            ('KIT GOLA', 'KIT GOLA/PUNHO'),
            ('MALHA', 'MALHA'),
            ('TECIDO', 'TECIDO PLANO'),
            ('ETIQUETA', 'ETIQUETAS'),
            ('CADAR', 'CADARCO/CORDAO'),
            ('ELAST', 'ELASTICOS'),
            ('SAQUI', 'EMBALAGEM'),
            ('CORDAO', 'CADARCO/CORDAO'),
            ('MOLET', 'MOLETOM'),
        ]

        categoria_atual = row['categoriaMP']
        descricao = row['descricaoComponente']

        for termo, nova_categoria in categorias:
            categoria_atual = self.__categoria(termo, descricao, nova_categoria, categoria_atual)

        return categoria_atual


    def obtendoUltimaAnalise_porPlano(self):
        '''Método que obtem a data e hora da ultima analise de acordo com o Plano escolhido'''

        sql = """
        select 
            "DataHora", "codPlano" 
        from 
            pcp."controleServicos"
        where 
            "codPlano"  = %s
            and "Servico" = 'AnaliseMateriais'
        order by 
            "DataHora" desc
        """

        conn = ConexaoPostgre.conexaoEngine()

        sql = pd.read_sql(sql, conn, params=(self.codPlano,))

        if sql.empty:

            return pd.DataFrame([{'Mensagem':f'Cálculo da Necessidade nunca foi calculado para o plano {self.codPlano}','status':False}])

        else:

            return pd.DataFrame([{'Mensagem':f'Último cálculo feito em {sql["DataHora"][0]}, deseja recalcular ?',"status":True}])



    def obtendoUltimaAnalise_porMPPlanoSimulacao(self):
        '''Método que obtem a data e hora da ultima analise de acordo com o Plano escolhido'''

        sql = """
        select 
            "DataHora", "codPlano" , "nomeSimulacao"
        from 
            pcp."controleServicos"
        where 
            "codPlano"  = %s
            and "Servico" = 'SimulacaoPlanoMP'
            and "nomeSimulacao" = %s
        order by 
            "DataHora" desc
        """

        conn = ConexaoPostgre.conexaoEngine()

        sql = pd.read_sql(sql, conn, params=(self.codPlano,self.nomeSimulacao))

        if sql.empty:

            return pd.DataFrame([{'Mensagem':f'Cálculo dessa Simulacao nunca foi calculado para o plano {self.codPlano}_{self.nomeSimulacao}','status':False}])

        else:

            return pd.DataFrame([{'Mensagem':f'Último dessa Simulacao feito em {sql["DataHora"][0]}, deseja recalcular ?',"status":True}])


    def atualizando_MPPlanoSimulacao(self):
        '''Método que atualiza a dataHora do Cálculo da Analise de Materiais '''


        insert = """
            insert into pcp."controleServicos" ("DataHora", "codPlano", "Servico", "nomeSimulacao" ) values ( %s , %s , 'SimulacaoPlanoMP', %s )
        """

        uptade = """
            update 
                pcp."controleServicos"
            set 
                "DataHora" = %s, "Servico" = 'SimulacaoPlanoMP'
            where 
                "codPlano" = %s 
                and "Servico" = 'SimulacaoPlanoMP'
                and "nomeSimulacao" = %s
        """

        consulta = self.obtendoUltimaAnalise_porMPPlanoSimulacao()

        if consulta['status'][0] == False:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert, (self.obterdiaAtual(), self.codPlano, self.nomeSimulacao))
                    conn.commit()

        else:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(uptade, (self.obterdiaAtual(), self.codPlano, self.nomeSimulacao))
                    conn.commit()




    def atualizando_InserindoCalAnalise(self):
        '''Método que atualiza a dataHora do Cálculo da Analise de Materiais '''


        insert = """
            insert into pcp."controleServicos" ("DataHora", "codPlano", "Servico" ) values ( %s , %s , 'AnaliseMateriais' )
        """

        uptade = """
            update 
                pcp."controleServicos"
            set 
                "DataHora" = %s, "Servico" = 'AnaliseMateriais'
            where 
                "codPlano" = %s 
                and "Servico" = 'AnaliseMateriais'
        """

        consulta = self.obtendoUltimaAnalise_porPlano()

        if consulta['status'][0] == False:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert, (self.obterdiaAtual(), self.codPlano))
                    conn.commit()

        else:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(uptade, (self.obterdiaAtual(), self.codPlano))
                    conn.commit()

    def obterdiaAtual(self):
        '''
        Método para obter a data e hora atual no fuso horário de São Paulo.
        :return:
            Data e hora no formato 'dd/mm/aaaa HH:MM'
        '''
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora_formatado = agora.strftime('%d/%m/%Y %H:%M')
        return agora_formatado

    def estrutura_ItensCongelada(self, simula = 'nao'):
        '''Metodo que extrai a analise de necessidades com congelamento '''
        caminho_absoluto2 = configApp.localProjeto
        produtos = Produtos.Produtos(self.codEmpresa)

        if simula == 'nao':
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/NecessidadePrevisaoCongelada{self.codPlano}.csv')
        else:
            # 1 CASO NAO TENHA SIMULACAO CALCULADA

            #1.1 Carrega a necessidade em csv congelado para ganhar performace
            Necessidade = pd.read_csv(f'{caminho_absoluto2}/dados/EstruturacaoPrevisao{self.codPlano}.csv')

            #1.2 Calcula uma nova Simulacao e agrupa por codReduzido
            sqlMetas = Tendencia_Plano.Tendencia_Plano(self.codEmpresa, self.codPlano,
                                                       self.consideraPedBloq, self.nomeSimulacao).simulacaoPeloNome()
            sqlMetas = sqlMetas.groupby(['codReduzido']).agg({
            "previcaoVendas":"first",
            "faltaProg (Tendencia)":"first",
                'nomeSimulacao': 'first'
            }).reset_index()

            #1.3 Renomeia as colunas das metas
            sqlMetas.rename(
                columns={
                    'faltaProg (Tendencia)': 'faltaProg (Tendencia)Simulacao',
                                 'previcaoVendas': 'previcaoVendas_Simulacao'
                },
                inplace=True)

            #1.4 Realiza um merge para obter um dataframe unico
            Necessidade['codReduzido'] = Necessidade['codReduzido'].astype(float).astype(int).astype(str)
            sqlMetas['codReduzido'] = sqlMetas['codReduzido'].astype(float).astype(int).astype(str)
            Necessidade = pd.merge(Necessidade, sqlMetas, on='codReduzido')


            # 1.5 Calcula o falta programar converido em materia prima
            Necessidade['faltaProg (Tendencia)Simulacao_MP'] = Necessidade['faltaProg (Tendencia)Simulacao'] * Necessidade['quantidade']
            # 1.6 Calcula odisponivelVendas converido em materia prima
            Necessidade['disponivelVendasMP'] = Necessidade['disponivel'] * Necessidade['quantidade']
            Necessidade['CodComponente'] = Necessidade['CodComponente'].astype(float).astype(int).astype(str)

            # GRAVANDO O CALCULO DA SIMULACAO

            Necessidade.to_csv(f'{caminho_absoluto2}/dados/EstruturacaoPrevisao{self.codPlano}_Simulacao{self.nomeSimulacao}.csv')
            # 1.7 Resume a necessidade agrupando por codigo componentente
            Necessidade = Necessidade.groupby(["CodComponente"]).agg(
                {"disponivelVendasMP": "sum",
                 "faltaProg (Tendencia)Simulacao_MP": "sum",
                 "descricaoComponente": 'first',
                 "unid": 'first'
                 }).reset_index()
            #1.8 Obtem os pedidos em aberto no compras e desconta os entregue parcial
            sqlAtendidoParcial = produtos.req_atendidoComprasParcial()
            sqlPedidos = produtos.pedidoComprasMP()
            sqlPedidos = pd.merge(sqlPedidos, sqlAtendidoParcial, on=['numero', 'seqitem'], how='left')
            sqlPedidos['qtAtendida'].fillna(0, inplace=True)

            #1.9 Realizando o tratamento do fator de conversao de compras dos componentes
            sqlPedidos['fatCon2'] = sqlPedidos['fatCon'].apply(self.__process_fator)
            sqlPedidos['qtdPedida'] = sqlPedidos['fatCon2'] * sqlPedidos['qtdPedida']
            sqlPedidos['SaldoPedCompras'] = sqlPedidos['qtdPedida'] - sqlPedidos['qtAtendida']

            # Congelando o dataFrame de Pedidos em aberto
            sqlPedidos.to_csv(f'{caminho_absoluto2}/dados/pedidosEmAberto.csv')

            sqlPedidos = sqlPedidos.groupby(["CodComponente"]).agg(
                {"SaldoPedCompras": "sum"}).reset_index()


            # Carregando as requisicoes em aberto
            sqlRequisicaoAberto = produtos.req_Materiais_aberto()
            # Congelando o dataFrame de Requisicoes em aberto
            caminho_absoluto2 = configApp.localProjeto
            sqlRequisicaoAberto.to_csv(f'{caminho_absoluto2}/dados/requisicoesEmAberto.csv')

            # Agrupando as requisicoes compromedito pelo CodComponente
            sqlRequisicaoAberto = sqlRequisicaoAberto.groupby(["CodComponente"]).agg(
                {"EmRequisicao": "sum"}).reset_index()

            sqlEstoque = produtos.estMateriaPrima()
            # Agrupando as requisicoes compromedito pelo CodComponente
            sqlEstoque = sqlEstoque.groupby(["CodComponente"]).agg(
                {"estoqueAtual": "sum"}).reset_index()

            # Mergem entre pedidos x requisicoes
            Necessidade = pd.merge(Necessidade, sqlPedidos, on='CodComponente', how='left')
            Necessidade = pd.merge(Necessidade, sqlRequisicaoAberto, on='CodComponente', how='left')
            Necessidade = pd.merge(Necessidade, sqlEstoque, on='CodComponente', how='left')
            Necessidade['SaldoPedCompras'].fillna(0, inplace=True)
            Necessidade['EmRequisicao'].fillna(0, inplace=True)
            Necessidade['estoqueAtual'].fillna(0, inplace=True)

            # calculando a necessidade do falta programar abatendo estoque + saldo compras + requisicoes
            Necessidade['Necessidade faltaProg (Tendencia)'] = (Necessidade['faltaProg (Tendencia)Simulacao_MP']) + Necessidade[
                'estoqueAtual'] + Necessidade['SaldoPedCompras'] - Necessidade['EmRequisicao']

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
                         'faltaProg (Tendencia)Simulacao_MP': '06-Necessidade faltaProg(Tendencia)',
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

            Necessidade = Necessidade.drop(columns=['disponivelVendasMP'])
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
















