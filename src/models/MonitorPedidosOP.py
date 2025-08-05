import os

import numpy
import pandas as pd

from src.configApp import configApp
from src.models.Pedidos_CSW import Pedidos_CSW
from src.models.Pedidos import Pedidos
from  src.models import Produtos, Produtos_CSW
from src.connection import ConexaoPostgre
from dotenv import load_dotenv, dotenv_values
import fastparquet as fp

class MonitorPedidosOP():
    '''Classe que gerencia o processo de Monitor de Pedidos'''

    def __init__(self, empresa, dataInicioVendas, dataFinalVendas, tipoDataEscolhida, dataInicioFat, dataFinalFat,
                 arrayRepresentantesExcluir='',
                 arrayEscolherRepresentante='', arrayescolhernomeCliente='', parametroClassificacao=None,
                 filtroDataEmissaoIni='', filtroDataEmissaoFim='',
                 analiseOPGarantia=None,
                 arrayOPSimuladas = ''):

        self.empresa = empresa  # Codigo da empresa utilizada no monitor
        self.dataInicioVendas = dataInicioVendas  # Início  de vendas : data de filtro de 'Emissao' do pedido de venda;
        self.dataFinalVendas = dataFinalVendas  # Fim  de vendas : data de filtro de 'Emissao' do pedido de venda;
        self.tipoDataEscolhida = tipoDataEscolhida  # Filtrar o criterio utilizado no tipo de data : DataEmissao  x DataPrevOriginal
        self.dataInicioFat = dataInicioFat  #
        self.dataFinalFat = dataFinalFat
        self.arrayRepresentantesExcluir = arrayRepresentantesExcluir
        self.arrayEscolherRepresentante = arrayEscolherRepresentante
        self.arrayescolhernomeCliente = arrayescolhernomeCliente
        self.parametroClassificacao = parametroClassificacao
        self.filtroDataEmissaoIni = filtroDataEmissaoIni
        self.filtroDataEmissaoFim = filtroDataEmissaoFim
        self.analiseOPGarantia = analiseOPGarantia
        self.descricaoArquivo = self.dataInicioFat + '_' + self.dataFinalFat
        self.arrayTipoNota = '1,2,3,4,5,6,7,8,10,24,92,201,1012,77,27,28,172,9998,66,67,233,237'  # Arrumar o Tipo de Nota 40
        self.arrayOPSimuladas = arrayOPSimuladas

        self.pedidos_CSW = Pedidos_CSW(self.empresa, '', self.filtroDataEmissaoIni, self.filtroDataEmissaoFim, self.dataInicioVendas, self.dataFinalVendas, self.dataInicioFat, self.dataFinalFat, self.arrayTipoNota)
        self.faturamento = Pedidos(self.empresa,'')
        self.produto_csw = Produtos_CSW.Produtos_CSW(self.empresa)

    def __preparacaoPedidosParaMonitor(self):
        '''Metodo utilizado para gerar o monitor de pedidos
        return:
        DataFrame
        '''



        # 1 - Carregar Os pedidos (etapa 1)
        if self.tipoDataEscolhida == 'DataEmissao':
            pedidos = self.pedidos_CSW.capaPedidos()
        else:
            pedidos = self.pedidos_CSW.capaPedidosDataFaturamento()

            # 1.1 - Verificar se tem representantes a serem excuidos da analise
        if self.arrayRepresentantesExcluir != '':
            Representante_excluir = self.arrayRepresentantesExcluir.split(', ')
            pedidos = pedidos[~pedidos['codRepresentante'].astype(str).isin(Representante_excluir)]

         # 1.2 - Verificar se o filtro esta aplicado para representantes excluisvos
        if self.arrayEscolherRepresentante != '':
            escolherRepresentante = self.arrayEscolherRepresentante.split(', ')
            pedidos = pedidos[pedidos['codRepresentante'].astype(str).isin(escolherRepresentante)]

        # 1.3 - Verificar se o filtro esta aplicado para clientes excluisvos
        if self.arrayescolhernomeCliente != '':
            # escolhernomeCliente = escolhernomeCliente.split(', ')
            pedidos = pedidos[pedidos['nome_cli'].str.contains(self.arrayescolhernomeCliente, case=False, na=False)]

        # 1.4 - Relaizar a combinacao com a capa da sugestao para verificar o status da sugestao do pedido
        statusSugestao = self.pedidos_CSW.capaSugestao()
        pedidos = pd.merge(pedidos, statusSugestao, on='codPedido', how='left')
        pedidos["StatusSugestao"].fillna('Nao Sugerido', inplace=True)
        pedidos["codSitSituacao"].fillna('0', inplace=True)

        # 2 - Filtrar Apenas Pedidos Não Bloqueados
        pedidosBloqueados = self.pedidos_CSW.pedidosBloqueados()
        pedidos = pd.merge(pedidos, pedidosBloqueados, on='codPedido', how='left')
        pedidos['situacaobloq'].fillna('Liberado', inplace=True)
        pedidos = pedidos[pedidos['situacaobloq'] == 'Liberado']


        return pedidos

    def __inserindoEntregas(self):

        pedidos = self.__preparacaoPedidosParaMonitor()

        # 3- Consulta de Embarques Enviados do pedido , utilizando a consulta de notas fiscais do ERP
        entregasFaturadas = self.pedidos_CSW.obtendoEntregas_Enviados()
        pedidos = pd.merge(pedidos, entregasFaturadas, on='codPedido', how='left')
        pedidos['entregas_enviadas'].fillna(0, inplace=True)


        # 4- Consulta de Embarques Solicitado pelo Cliente , informacao extraida do ERP
        entregasSolicitadas = self.pedidos_CSW.obtendoEntregasSolicitadas()
        pedidos = pd.merge(pedidos, entregasSolicitadas, on='codPedido', how='left')
        pedidos['entregas_Solicitadas'].fillna(0, inplace=True)

        return pedidos


    def __explodindo_Skus(self):
        '''Metodo privado que explode os pedidos ao nivel de sku '''

        pedidos = self.__inserindoEntregas()


        # 5 - Explodir os pedidos no nivel sku
        if self.tipoDataEscolhida  == 'DataEmissao':
            sku = self.faturamento.listagemPedidosSku('',self.dataInicioVendas, self.dataFinalVendas,'','',True)

        elif self.filtroDataEmissaoFim != '' and self.filtroDataEmissaoIni != '' and self.tipoDataEscolhida  != 'DataEmissao':
            sku = self.faturamento.listagemPedidosSku('',self.filtroDataEmissaoIni, self.filtroDataEmissaoFim,self.dataInicioFat,self.dataFinalFat,True)

        else:
            sku = self.faturamento.listagemPedidosSku('',self.filtroDataEmissaoIni, self.filtroDataEmissaoFim,self.dataInicioFat,self.dataFinalFat,True)


        produto = Produtos.Produtos(self.empresa)

        # 5.1 - Considerando somente a qtdePedida maior que 0
        pedidos = pd.merge(pedidos, sku, on='codPedido', how='left')
        pedidos = pd.merge(pedidos, produto.estruturaSku(), on='codProduto', how='left')
        pedidos['QtdSaldo'] = pedidos['qtdePedida'] - pedidos['qtdeFaturada'] - pedidos['qtdeSugerida']
        pedidos['QtdSaldo'].fillna(0, inplace=True)
        pedidos['QtdSaldo'] = pedidos['QtdSaldo'].astype(int)


        return pedidos


    def __adicionando_inf_estoques(self):
        '''Metodo privado que realiza a adicao de estoque ao SUPER DATAFRAME do Monitor de Pedidos'''

        pedidos = self.__explodindo_Skus()


        # 6 Consultando n banco de dados do ERP o saldo de estoque

        if self.arrayOPSimuladas != '':
            estoque = self.produto_csw.estoqueReduzido()

            conn2 = ConexaoPostgre.conexaoEngine()

            ops = self.arrayOPSimuladas

            # Dividir a string em partes
            parts = ops.split(", ")

            # Adicionar aspas simples a cada parte
            ops = [f"'{part}'" for part in parts]

            # Combinar as partes de volta em uma única string
            ops = ", ".join(ops)
            query = """
               select "codreduzido" as "codProduto","total_pcs" as "estoqueAtual", 0 as "estReservPedido" from pcp.ordemprod
                where numeroop in ( """ + ops + """)"""

            simulacao = pd.read_sql(query, conn2)
            print('minha simulacao')
            print(simulacao)

            estoque = pd.concat([simulacao, estoque])

            estoque = estoque.groupby('codProduto').agg({
                "estoqueAtual": 'sum',
                "estReservPedido": 'sum'}).reset_index()


        else:
            estoque = self.produto_csw.estoqueReduzido()

        pedidos = pd.merge(pedidos, estoque, on='codProduto', how='left')

        return pedidos


    def __atualizando_data_previsao_nova(self):

        pedidos = self.__adicionando_inf_estoques()

        # 7 Calculando a nova data de Previsao do pedido
        pedidos['dias_a_adicionar'] = pd.to_timedelta(pedidos['entregas_enviadas'] * 15,
                                                      unit='d')  # Converte a coluna de inteiros para timedelta
        pedidos['dataPrevAtualizada'] = pd.to_datetime(pedidos['dataPrevFat'], errors='coerce',
                                                       infer_datetime_format=True)
        pedidos['dataPrevAtualizada'] = pedidos['dataPrevAtualizada'] + pedidos['dias_a_adicionar']
        pedidos['dataPrevAtualizada'].fillna('-', inplace=True)

        pedidos = self.__classificacao(pedidos, self.parametroClassificacao)

        return pedidos

    def __classificando_pedidosMonitor(self):

        pedidos = self.__atualizando_data_previsao_nova()
        pedidos = self.__classificacao(pedidos, self.parametroClassificacao)

        # 9 Contando o numero de ocorrencias acumulado do sku no DataFrame
        pedidos = pedidos[pedidos['vlrSaldo'] > 0]
        pedidos['Sku_acumula'] = pedidos.groupby('codProduto').cumcount() + 1

        return pedidos


    def __classificacao(self, pedidos, parametro):

        if parametro == 'Faturamento':
            # Define os valores de 'codSitSituacao' com base na condição para Faturamento
            pedidos.loc[(pedidos['codSitSituacao'] == '0') | (
                        pedidos['codSitSituacao'] == '1'), 'codSitSituacao'] = '2-InicioFila'
            pedidos.loc[(pedidos['codSitSituacao'] != '2-InicioFila'), 'codSitSituacao'] = '1-FimFila'
            pedidos = pedidos.sort_values(by=['codSitSituacao', 'vlrSaldo'], ascending=False)
        elif parametro == 'DataPrevisao':
            # Define os valores de 'codSitSituacao' com base na condição para DataPrevisao
            pedidos.loc[(pedidos['codSitSituacao'] == '0') | (
                        pedidos['codSitSituacao'] == '1'), 'codSitSituacao'] = '1-InicioFila'
            pedidos.loc[(pedidos['codSitSituacao'] != '1-InicioFila'), 'codSitSituacao'] = '2-FimFila'
            pedidos = pedidos.sort_values(by=['codSitSituacao', 'dataPrevAtualizada'], ascending=True)
        return pedidos


    def __calculo_necessidade_estoque(self):

        pedidos = self.__classificando_pedidosMonitor()


        # 10.1 Obtendo o Estoque Liquido para o calculo da necessidade
        pedidos['EstoqueLivre'] = pedidos['estoqueAtual'] - pedidos['estReservPedido']
        # 10.2 Obtendo a necessidade de estoque
        pedidos['Necessidade'] = pedidos.groupby('codProduto')['QtdSaldo'].cumsum()
        # 10.3 0 Obtendo a Qtd que antende para o pedido baseado no estoque
        pedidos['Qtd Atende'] = pedidos['QtdSaldo'].where(pedidos['Necessidade'] <= pedidos['EstoqueLivre'], 0)
        pedidos.loc[pedidos['qtdeSugerida'] > 0, 'Qtd Atende'] = pedidos['qtdeSugerida']
        pedidos['Qtd Atende'] = pedidos['Qtd Atende'].astype(int)

        # 11.1 Separando os pedidos a nivel pedido||engenharia||cor
        pedidos["Pedido||Prod.||Cor"] = pedidos['codPedido'].str.cat([pedidos['codItemPai'], pedidos['codCor']],
                                                                     sep='||')
        # 11.2  Calculando a necessidade a nivel de grade Pedido||Prod.||Cor
        pedidos['Saldo +Sugerido'] = pedidos['QtdSaldo'] + pedidos['qtdeSugerida']
        pedidos['Saldo Grade'] = pedidos.groupby('Pedido||Prod.||Cor')['Saldo +Sugerido'].transform('sum')
        # 12 obtendo a Qtd que antende para o pedido baseado no estoque e na grade
        pedidos['X QTDE ATENDE'] = pedidos.groupby('Pedido||Prod.||Cor')['Qtd Atende'].transform('sum')
        pedidos['Qtd Atende por Cor'] = pedidos['Saldo +Sugerido'].where(
            pedidos['Saldo Grade'] == pedidos['X QTDE ATENDE'], 0)

        pedidos['Qtd Atende por Cor'] = pedidos['Qtd Atende por Cor'].astype(int)


        return pedidos



    def __calculando_pedidos_comFechamentoAcumulado(self):

        pedidos = self.__calculo_necessidade_estoque()

        # 13- Indicador de % que fecha no pedido a nivel de grade Pedido||Prod.||Cor'

        pedidos['Fecha Acumulado'] = pedidos.groupby('codPedido')['Qtd Atende por Cor'].cumsum().round(2)
        pedidos['Saldo +Sugerido_Sum'] = pedidos.groupby('codPedido')['Saldo +Sugerido'].transform('sum')
        pedidos['% Fecha Acumulado'] = (pedidos['Fecha Acumulado'] / pedidos['Saldo +Sugerido_Sum']).round(2) * 100
        pedidos['% Fecha Acumulado'] = pedidos['% Fecha Acumulado'].astype(str)
        pedidos['% Fecha Acumulado'] = pedidos['% Fecha Acumulado'].str.slice(0, 4)
        pedidos['% Fecha Acumulado'] = pedidos['% Fecha Acumulado'].astype(float)

        # 14 - Encontrando a Marca desejada
        pedidos['codItemPai'] = pedidos['codItemPai'].astype(str)
        pedidos['MARCA'] = pedidos['codItemPai'].apply(lambda x: x[:3])
        pedidos['MARCA'] = numpy.where(
            (pedidos['codItemPai'].str[:3] == '102') | (pedidos['codItemPai'].str[:3] == '202'), 'M.POLLO', 'PACO')


        return pedidos



    def __categorizar_produto(self):
        pedidos = self.__calculando_pedidos_comFechamentoAcumulado()

        def categorizar_produto(produto):
            if 'JAQUETA' in produto:
                return 'AGASALHOS'
            elif 'BLUSAO' in produto:
                return 'AGASALHOS'
            elif 'TSHIRT' in produto:
                return 'CAMISETA'
            elif 'POLO' in produto:
                return 'POLO'
            elif 'SUNGA' in produto:
                return 'SUNGA'
            elif 'CUECA' in produto:
                return 'CUECA'
            elif 'CALCA/BER MOLETOM  ' in produto:
                return 'CALCA/BER MOLETOM '
            elif 'CAMISA' in produto:
                return 'CAMISA'
            elif 'SHORT' in produto:
                return 'BOARDSHORT'
            elif 'TRICOT' in produto:
                return 'TRICOT'
            elif 'BABY' in produto:
                return 'CAMISETA'
            elif 'BATA' in produto:
                return 'CAMISA'
            elif 'CALCA' in produto:
                return 'CALCA/BER MOLETOM'
            elif 'CARTEIRA' in produto:
                return 'ACESSORIOS'
            elif 'BONE' in produto:
                return 'ACESSORIOS'
            elif 'TENIS' in produto:
                return 'CALCADO'
            elif 'CHINELO' in produto:
                return 'CALCADO'
            elif 'MEIA' in produto:
                return 'ACESSORIOS'
            elif 'BLAZER' in produto:
                return 'AGASALHOS'
            elif 'CINTO' in produto:
                return 'ACESSORIOS'
            elif 'REGATA' in produto:
                return 'ACESSORIOS'
            elif 'BERMUDA' in produto:
                return 'CALCA/BER MOLETOM'
            elif 'COLETE' in produto:
                return 'AGASALHOS'
            elif 'NECESSA' in produto:
                return 'ACESSORIOS'
            elif 'CARTA' in produto:
                return 'ACESSORIOS'
            elif 'SACOL' in produto:
                return 'ACESSORIOS'
            else:
                return '-'

        # 15 - Encontrando a categoria do produto
        try:
            pedidos['CATEGORIA'] = pedidos['nomeSKU'].apply(categorizar_produto)
            # pedidos['CATEGORIA'] = pedidos.apply(lambda row: categorizar_produto(row['nomeSKU']),axis=1)
        except:
            pedidos['CATEGORIA'] = '-'

        return pedidos

    def __adicionando_conf_embarques(self):

        pedidos = self.__categorizar_produto()

        conn = ConexaoPostgre.conexaoEngine()

        consultar = pd.read_sql(
            """Select * from pcp.monitor_fat_dados """, conn)

        consultar['Entregas Restantes'] = consultar['Entregas Restantes'].astype(str)

        # 16.1 Encontrando o numero restante de entregas
        pedidos['Entregas Restantes'] = pedidos['entregas_Solicitadas'] - pedidos['entregas_enviadas']
        # pedidos['Entregas Restantes'] = pedidos.apply(lambda row: 1 if row['entregas_Solicitadas'] <= row['entregas_enviadas'] else row['Entregas Restantes'], axis=1)
        pedidos.loc[pedidos['entregas_Solicitadas'] <= pedidos['entregas_enviadas'], 'Entregas Restantes'] = 1

        pedidos['Entregas Restantes'] = pedidos['Entregas Restantes'].astype(str)
        pedidos['Entregas Restantes'] = pedidos['Entregas Restantes'].str.replace('.0', '')
        pedidos = pd.merge(pedidos, consultar, on='Entregas Restantes', how='left')


        consultar2 = pd.read_sql(
                """
                Select "Opção" as "CATEGORIA", "Status" from pcp.monitor_check_status 
                """, conn)

        pedidos = pd.merge(pedidos, consultar2, on='CATEGORIA', how='left')
        pedidos.loc[pedidos['Status'] != '1', 'Qtd Atende por Cor'] = 0
        pedidos.loc[pedidos['Status'] != '1', 'Qtd Atende'] = 0


        return pedidos


    def __encontrando_percentual_atendePedidos(self):

        pedidos = self.__adicionando_conf_embarques()

        # 18 - Encontrando no pedido o percentual que atende a distribuicao
        pedidos['% Fecha pedido'] = (pedidos.groupby('codPedido')['Qtd Atende por Cor'].transform('sum')) / (
            pedidos.groupby('codPedido')['Saldo +Sugerido'].transform('sum'))
        pedidos['% Fecha pedido'] = pedidos['% Fecha pedido'] * 100
        pedidos['% Fecha pedido'] = pedidos['% Fecha pedido'].astype(float).round(2)
        # etapa18 = controle.salvarStatus_Etapa18(rotina, ip, etapa17,'Encontrando no pedido o percentual que atende a distribuicao')  # Registrar etapa no controlador

        # 19 - Encontrando os valores que considera na ditribuicao
        pedidos['ValorMin'] = pedidos['ValorMin'].astype(float)
        pedidos['ValorMax'] = pedidos['ValorMax'].astype(float)
        condicoes = [(pedidos['% Fecha pedido'] >= pedidos['ValorMin']) &
                     (pedidos['% Fecha pedido'] <= pedidos['ValorMax']),
                     (pedidos['% Fecha pedido'] > pedidos['ValorMax']) &
                     (pedidos['% Fecha Acumulado'] <= pedidos['ValorMax']),
                     (pedidos['% Fecha pedido'] > pedidos['ValorMax']) &
                     (pedidos['% Fecha Acumulado'] > pedidos['ValorMax']),
                     (pedidos['% Fecha pedido'] < pedidos['ValorMin'])
                     # adicionar mais condições aqui, se necessário
                     ]
        valores = ['SIM', 'SIM', 'SIM(Redistribuir)', 'NAO']  # definir os valores correspondentes
        pedidos['Distribuicao'] = numpy.select(condicoes, valores, default=True)


        return pedidos

    def avaliar_grupo(self,df_grupo):
        return len(set(df_grupo)) == 1

    def __avaliando_distribuicao(self):
        pedidos = self.__encontrando_percentual_atendePedidos()



        df_resultado = pedidos.loc[:, ['Pedido||Prod.||Cor', 'Distribuicao']]
        df_resultado = df_resultado.groupby('Pedido||Prod.||Cor')['Distribuicao'].apply(self.avaliar_grupo).reset_index()
        df_resultado.columns = ['Pedido||Prod.||Cor', 'Resultado']
        df_resultado['Resultado'] = df_resultado['Resultado'].astype(str)

        pedidos = pd.merge(pedidos, df_resultado, on='Pedido||Prod.||Cor', how='left')

        # 19.1: Atualizando a coluna 'Distribuicao' diretamente
        condicao = (pedidos['Resultado'] == 'False') & (
                (pedidos['Distribuicao'] == 'SIM') & (pedidos['Qtd Atende por Cor'] > 0))
        pedidos.loc[condicao, 'Distribuicao'] = 'SIM(Redistribuir)'
        # etapa21 = controle.salvarStatus_Etapa21(rotina, ip, etapa20,'Encontrando no pedido o percentual que atende a distribuicao')  # Registrar etapa no controlador

        # 20- Obtendo valor atente por cor
        pedidos['Valor Atende por Cor'] = pedidos['Qtd Atende por Cor'] * pedidos['PrecoLiquido']
        pedidos['Valor Atende por Cor'] = pedidos['Valor Atende por Cor'].astype(float).round(2)
        # etapa22 = controle.salvarStatus_Etapa22(rotina, ip, etapa21,'Obtendo valor atente por cor')  # Registrar etapa no controlador

        # 21 Identificando a Quantidade Distribuida
        # pedidos['Qnt. Cor(Distrib.)'] = pedidos.apply(lambda row: row['Qtd Atende por Cor'] if row['Distribuicao'] == 'SIM' else 0, axis=1)
        pedidos['Qnt. Cor(Distrib.)'] = pedidos['Qtd Atende por Cor'].where(pedidos['Distribuicao'] == 'SIM', 0)

        pedidos['Qnt. Cor(Distrib.)'] = pedidos['Qnt. Cor(Distrib.)'].astype(int)
        # etapa23 = controle.salvarStatus_Etapa23(rotina, ip, etapa22, 'Obtendo valor atente por cor')#Registrar etapa no controlador

        # 22 Obtendo valor atente por cor Distribuida
        # pedidos['Valor Atende por Cor(Distrib.)'] = pedidos.apply(lambda row: row['Valor Atende por Cor'] if row['Distribuicao'] == 'SIM' else 0, axis=1)
        pedidos['Valor Atende por Cor(Distrib.)'] = pedidos['Valor Atende por Cor'].where(
            pedidos['Distribuicao'] == 'SIM', 0)
        pedidos['Valor Atende'] = pedidos['Qtd Atende'] * pedidos['PrecoLiquido']
        pedidos['Valor Atende'] = pedidos['Valor Atende'].astype(float).round(2)
        pedidos.drop(['situacaobloq', 'dias_a_adicionar', 'Resultado'], axis=1, inplace=True)

        # etapa24 = controle.salvarStatus_Etapa24(rotina, ip, etapa23, 'Obtendo valor atente por cor Distribuida')#Registrar etapa no controlador
        pedidos['dataPrevAtualizada'] = pedidos['dataPrevAtualizada'].dt.strftime('%d/%m/%Y')
        pedidos["descricaoCondVenda"].fillna('-', inplace=True)
        pedidos["ultimo_fat"].fillna('-', inplace=True)
        pedidos["Status"].fillna('-', inplace=True)


        return pedidos



    def __ciclo_de_calculo_monitorPedidos(self):

        pedidos = self.__avaliando_distribuicao()

        # Ciclo 2
        situacao = pedidos.groupby('codPedido')['Valor Atende por Cor(Distrib.)'].sum().reset_index()
        situacao = situacao[situacao['Valor Atende por Cor(Distrib.)'] > 0]
        situacao.columns = ['codPedido', 'totalPçDis']
        pedidos = pd.merge(pedidos, situacao, on='codPedido', how='left')
        pedidos.fillna(0, inplace=True)

        pedidos1 = pedidos[pedidos['totalPçDis'] == 0]
        pedidos1['SituacaoDistrib'] = 'Redistribui'
        pedidos1 = self.__ciclo2(pedidos1, self.avaliar_grupo, self.arrayOPSimuladas)
        pedidos2 = pedidos[pedidos['totalPçDis'] > 0]
        pedidos2['SituacaoDistrib'] = 'Distribuido1'

        pedidos = pd.concat([pedidos1, pedidos2])



        return pedidos

    def __ciclo2(self, pedidos, avaliar_grupo, arrayOPSimuladas):
        ###### O Ciclo2 e´usado para redistribuir as quantidades dos skus  que nao conseguiram atender na distribuicao dos pedidos no primeiro ciclo.
        # etapa 1: recarregando estoque

        if self.arrayOPSimuladas != '':
            estoque = self.produto_csw.estoqueReduzido()

            conn2 = ConexaoPostgre.conexaoEngine()

            ops = self.arrayOPSimuladas

            # Dividir a string em partes
            parts = ops.split(", ")

            # Adicionar aspas simples a cada parte
            ops = [f"'{part}'" for part in parts]

            # Combinar as partes de volta em uma única string
            ops = ", ".join(ops)
            query = """
                      select "codreduzido" as "codProduto","total_pcs" as "estoqueAtual", 0 as "estReservPedido" from pcp.ordemprod
                       where numeroop in ( """ + ops + """)"""

            simulacao = pd.read_sql(query, conn2)
            print('minha simulacao')
            print(simulacao)

            estoque = pd.concat([simulacao, estoque])

            estoque = estoque.groupby('codProduto').agg({
                "estoqueAtual": 'sum',
                "estReservPedido": 'sum'}).reset_index()


        else:
            estoque = self.produto_csw.estoqueReduzido()




        print('verificar se foi pra redistribuicao')

        pedidos1 = pedidos[pedidos['StatusSugestao'] == 'Nao Sugerido']
        pedidos2 = pedidos[pedidos['StatusSugestao'] != 'Nao Sugerido']
        print('testando os pedios 2 no cilco')
        print(pedidos2[pedidos2['codPedido'] == '323256'])

        pedidos1['codProduto'].fillna(0, inplace=True)
        pedidos1['codProduto'] = pedidos1['codProduto'].astype(str)

        SKUnovaReserva = pedidos1.groupby('codProduto').agg({'Qnt. Cor(Distrib.)': 'sum'}).reset_index()

        pedidos1.drop(['EstoqueLivre', 'estoqueAtual', 'estReservPedido',
                       'Necessidade', 'Qtd Atende', 'Saldo +Sugerido',
                       'Saldo Grade', 'X QTDE ATENDE', 'Qtd Atende por Cor', 'Fecha Acumulado',
                       'Saldo +Sugerido_Sum', '% Fecha Acumulado', '% Fecha pedido', 'Distribuicao',
                       'Valor Atende por Cor', 'Qnt. Cor(Distrib.)'
                          , 'Valor Atende por Cor(Distrib.)', 'Valor Atende', 'totalPçDis', 'SituacaoDistrib'], axis=1,
                      inplace=True)

        # 2.1 Somando todas as cores que conseguiu distriubuir no ciclo 1 para depois abater
        SKUnovaReserva.rename(columns={'Qnt. Cor(Distrib.)': 'ciclo1'}, inplace=True)
        estoque['codProduto'] = estoque['codProduto'].astype(str)
        estoque2 = pd.merge(estoque, SKUnovaReserva, on='codProduto', how='left')

        # Etapa3 filtrando somente os pedidos nao distibuidos e fazendo o merge com o estoque
        pedidos1 = pd.merge(pedidos1, estoque2, on='codProduto', how='left')
        pedidos1['EstoqueLivre'] = pedidos1['estoqueAtual'] - pedidos1['estReservPedido'] - pedidos1['ciclo1']

        # Etapa 4 - Calculando a Nova Necessidade e descobrindo o quanto atente por cor
        pedidos1['Necessidade'] = pedidos1.groupby('codProduto')['QtdSaldo'].cumsum()
        pedidos1['Qtd Atende'] = pedidos1['QtdSaldo'].where(pedidos1['Necessidade'] <= pedidos1['EstoqueLivre'], 0)
        pedidos1.loc[pedidos1['qtdeSugerida'] > 0, 'Qtd Atende'] = pedidos1['qtdeSugerida']
        pedidos1['Qtd Atende'] = pedidos1['Qtd Atende'].astype(int)
        pedidos1['Saldo +Sugerido'] = pedidos1['QtdSaldo'] + pedidos1['qtdeSugerida']
        pedidos1['Saldo Grade'] = pedidos1.groupby('Pedido||Prod.||Cor')['Saldo +Sugerido'].transform('sum')
        pedidos1['X QTDE ATENDE'] = pedidos1.groupby('Pedido||Prod.||Cor')['Qtd Atende'].transform('sum')
        pedidos1['Qtd Atende por Cor'] = pedidos1['Saldo +Sugerido'].where(
            pedidos1['Saldo Grade'] == pedidos1['X QTDE ATENDE'], 0)
        pedidos1['Qtd Atende por Cor'] = pedidos1['Qtd Atende por Cor'].astype(int)

        # Etapa 5: Encontrando o novo % Fecha Acumalado para o ciclo2
        pedidos1['Fecha Acumulado'] = pedidos1.groupby('codPedido')['Qtd Atende por Cor'].cumsum().round(2)
        pedidos1['Saldo +Sugerido_Sum'] = pedidos1.groupby('codPedido')['Saldo +Sugerido'].transform('sum')
        pedidos1['% Fecha Acumulado'] = (pedidos1['Fecha Acumulado'] / pedidos1['Saldo +Sugerido_Sum']).round(2) * 100
        pedidos1['% Fecha Acumulado'] = pedidos1['% Fecha Acumulado'].astype(str)
        pedidos1['% Fecha Acumulado'] = pedidos1['% Fecha Acumulado'].str.slice(0, 4)
        pedidos1['% Fecha Acumulado'] = pedidos1['% Fecha Acumulado'].astype(float)

        pedidos1['% Fecha pedido'] = (pedidos1.groupby('codPedido')['Qtd Atende por Cor'].transform('sum')) / (
            pedidos1.groupby('codPedido')['Saldo +Sugerido'].transform('sum'))
        pedidos1['% Fecha pedido'] = pedidos1['% Fecha pedido'] * 100
        pedidos1['% Fecha pedido'] = pedidos1['% Fecha pedido'].astype(float).round(2)

        # Etapa6: Obtendo novos valores para a distribuicao
        pedidos1['ValorMin'] = pedidos1['ValorMin'].astype(float)
        pedidos1['ValorMax'] = pedidos1['ValorMax'].astype(float)
        condicoes = [(pedidos1['% Fecha pedido'] >= pedidos1['ValorMin']) &
                     (pedidos1['% Fecha pedido'] <= pedidos1['ValorMax']),
                     (pedidos1['% Fecha pedido'] > pedidos1['ValorMax']) &
                     (pedidos1['% Fecha Acumulado'] <= pedidos1['ValorMax']),
                     (pedidos1['% Fecha pedido'] > pedidos1['ValorMax']) &
                     (pedidos1['% Fecha Acumulado'] > pedidos1['ValorMax']),
                     (pedidos1['% Fecha pedido'] < pedidos1['ValorMin'])
                     # adicionar mais condições aqui, se necessário
                     ]
        valores = ['SIM', 'SIM', 'SIM(Redistribuir)', 'NAO']  # definir os valores correspondentes
        pedidos1['Distribuicao'] = numpy.select(condicoes, valores, default=True)

        # Etapa 7: Avaliando se no nivel de pedido||sku||cor possui situacao de quebra
        df_resultado = pedidos1.loc[:, ['Pedido||Prod.||Cor', 'Distribuicao']]
        df_resultado = df_resultado.groupby('Pedido||Prod.||Cor')['Distribuicao'].apply(avaliar_grupo).reset_index()
        df_resultado.columns = ['Pedido||Prod.||Cor', 'Resultado']
        df_resultado['Resultado'] = df_resultado['Resultado'].astype(str)
        pedidos1 = pd.merge(pedidos1, df_resultado, on='Pedido||Prod.||Cor', how='left')
        # 7.1 Aplicando nova situacao no redistriuir
        condicao = (pedidos1['Resultado'] == 'False') & (
                (pedidos1['Distribuicao'] == 'SIM') & (pedidos1['Qtd Atende por Cor'] > 0))
        pedidos1.loc[condicao, 'Distribuicao'] = 'SIM(Redistribuir)'

        # 8- Encontradno os novos valores para o ciclo2:
        pedidos1['Valor Atende por Cor'] = pedidos1['Qtd Atende por Cor'] * pedidos1['PrecoLiquido']
        pedidos1['Valor Atende por Cor'] = pedidos1['Valor Atende por Cor'].astype(float).round(2)
        pedidos1['Qnt. Cor(Distrib.)'] = pedidos1['Qtd Atende por Cor'].where(pedidos1['Distribuicao'] == 'SIM', 0)
        pedidos1['Qnt. Cor(Distrib.)'] = pedidos1['Qnt. Cor(Distrib.)'].astype(int)
        pedidos1['Valor Atende por Cor(Distrib.)'] = pedidos1['Valor Atende por Cor'].where(
            pedidos1['Distribuicao'] == 'SIM', 0)
        pedidos1['Valor Atende'] = pedidos1['Qtd Atende'] * pedidos1['PrecoLiquido']
        pedidos1['Valor Atende'] = pedidos1['Valor Atende'].astype(float).round(2)

        situacao = pedidos1.groupby('codPedido')['Valor Atende por Cor(Distrib.)'].sum().reset_index()
        situacao = situacao[situacao['Valor Atende por Cor(Distrib.)'] > 0]
        situacao.columns = ['codPedido', 'totalPçDis']
        pedidos1 = pd.merge(pedidos1, situacao, on='codPedido', how='left')

        pedidos1['SituacaoDistrib'] = numpy.where(pedidos1['totalPçDis'] > 0, 'Distribuido2', 'Nao Redistribui')

        pedidosNovo = pd.concat([pedidos1, pedidos2])

        return pedidosNovo




    def __salvando_e_finalizando_calculo_monitor(self):
        pedidos = self.__ciclo_de_calculo_monitorPedidos()

        # 23- Salvando os dados gerados em csv
        # retirar as seguintes colunas: StatusSugestao, situacaobloq, dias_a_adicionar, Resultado    monitor.fillna('', inplace=True)
        pedidos['codProduto'] = pedidos['codProduto'].astype(str)
        pedidos['codCor'] = pedidos['codCor'].astype(str)
        pedidos['nomeSKU'] = pedidos['nomeSKU'].astype(str)
        pedidos['Pedido||Prod.||Cor'] = pedidos['Pedido||Prod.||Cor'].astype(str)

        env_path = configApp.localProjeto
        load_dotenv(f'{env_path}/_ambiente.env')
        caminhoAbsoluto = os.getenv('CAMINHO')
        fp.write(f'{caminhoAbsoluto}/dados/monitorSimulacao.parquet', pedidos)

        # etapa25 = controle.salvarStatus_Etapa25(rotina, ip, etapa24, 'Salvando os dados gerados no postgre')#Registrar etapa no controlador
        return pedidos

    def gatinlho_de_disparo_monitor(self):

        pedidos = self.__salvando_e_finalizando_calculo_monitor()

        pedidos['codPedido'] = pedidos['codPedido'].astype(str)
        pedidos['codCliente'] = pedidos['codCliente'].astype(str)
        pedidos["StatusSugestao"].fillna('-', inplace=True)
        pedidos = pedidos.groupby('codPedido').agg({
            "MARCA": 'first',
            "codTipoNota": 'first',
            "dataEmissao": 'first',
            "dataPrevFat": 'first',
            "dataPrevAtualizada": 'first',
            "codCliente": 'first',
            # "razao": 'first',
            "vlrSaldo": 'first',
            # "descricaoCondVenda": 'first',
            "entregas_Solicitadas": 'first',
            "entregas_enviadas": 'first',
            "qtdPecasFaturadas": 'first',
            'Saldo +Sugerido': 'sum',
            "ultimo_fat": "first",
            "Qtd Atende": 'sum',
            'QtdSaldo': 'sum',
            'Qtd Atende por Cor': 'sum',
            'Valor Atende por Cor': 'sum',
            # 'Valor Atende': 'sum',
            'StatusSugestao': 'first',
            'Valor Atende por Cor(Distrib.)': 'sum',
            'Qnt. Cor(Distrib.)': 'sum',
            'SituacaoDistrib': 'first'
            # 'observacao': 'first'
        }).reset_index()

        pedidos['%'] = pedidos['Qnt. Cor(Distrib.)'] / (pedidos['Saldo +Sugerido'])
        pedidos['%'] = pedidos['%'] * 100
        pedidos['%'] = pedidos['%'].round(0)
        print(pedidos[pedidos['codPedido'] == '323256'])

        pedidos.rename(columns={'MARCA': '01-MARCA', "codPedido": "02-Pedido",
                                "codTipoNota": "03-tipoNota", "dataPrevFat": "04-Prev.Original",
                                "dataPrevAtualizada": "05-Prev.Atualiz", "codCliente": "06-codCliente",
                                "vlrSaldo": "08-vlrSaldo", "entregas_Solicitadas": "09-Entregas Solic",
                                "entregas_enviadas": "10-Entregas Fat",
                                "ultimo_fat": "11-ultimo fat", "qtdPecasFaturadas": "12-qtdPecas Fat",
                                "Qtd Atende": "13-Qtd Atende", "QtdSaldo": "14- Qtd Saldo",
                                "Qnt. Cor(Distrib.)": "21-Qnt Cor(Distrib.)", "%": "23-% qtd cor",
                                "StatusSugestao": "18-Sugestao(Pedido)", "Qtd Atende por Cor": "15-Qtd Atende p/Cor",
                                "Valor Atende por Cor": "16-Valor Atende por Cor",
                                "Valor Atende por Cor(Distrib.)": "22-Valor Atende por Cor(Distrib.)"}, inplace=True)

        pedidos = pedidos.sort_values(by=['23-% qtd cor', '08-vlrSaldo'],
                                      ascending=False)  # escolher como deseja classificar
        pedidos["10-Entregas Fat"].fillna(0, inplace=True)
        pedidos["09-Entregas Solic"].fillna(0, inplace=True)

        pedidos["11-ultimo fat"].fillna('-', inplace=True)
        pedidos["05-Prev.Atualiz"].fillna('-', inplace=True)
        pedidos.fillna(0, inplace=True)

        pedidos["16-Valor Atende por Cor"] = pedidos["16-Valor Atende por Cor"].round(2)
        pedidos["22-Valor Atende por Cor(Distrib.)"] = pedidos["22-Valor Atende por Cor(Distrib.)"].round(2)

        saldo = pedidos['08-vlrSaldo'].sum()
        TotalQtdCor = pedidos['15-Qtd Atende p/Cor'].sum()
        TotalValorCor = pedidos['16-Valor Atende por Cor'].sum()
        TotalValorCor = TotalValorCor.round(2)

        totalPedidos = pedidos['02-Pedido'].count()
        PedidosDistribui = pedidos[pedidos['23-% qtd cor'] > 0]
        PedidosDistribui = PedidosDistribui['02-Pedido'].count()

        pedidosRedistribuido = pedidos[pedidos['SituacaoDistrib'] == 'Distribuido2']
        pedidosRedistribuido = pedidosRedistribuido['SituacaoDistrib'].count()

        TotalQtdCordist = pedidos['21-Qnt Cor(Distrib.)'].sum()
        TotalValorCordist = pedidos['22-Valor Atende por Cor(Distrib.)'].sum()
        TotalValorCordist = TotalValorCordist.round(2)

        # Agrupando os clientes
        # Função para concatenar os valores agrupados
        def concat_values(group):
            return '/'.join(str(x) for x in group)

        # Agrupar e aplicar a função de concatenação
        result = pedidos.groupby('06-codCliente')['02-Pedido'].apply(concat_values).reset_index()
        # Renomear as colunas
        result.columns = ['06-codCliente', 'Agrupamento']
        pedidos = pd.merge(pedidos, result, on='06-codCliente', how='left')

        dados = {
            '0-Status': True,
            '1-Total Qtd Atende por Cor': f'{TotalQtdCor} Pçs',
            '2-Total Valor Valor Atende por Cor': f'{TotalValorCor}',
            '3-Total Qtd Cor(Distrib.)': f'{TotalQtdCordist} Pçs',
            '4-Total Valor Atende por Cor(Distrib.)': f'{TotalValorCordist}',
            '5-Valor Saldo Restante': f'{saldo}',
            '5.1-Total Pedidos': f'{totalPedidos}',
            '5.2-Total Pedidos distribui': f'{PedidosDistribui},({pedidosRedistribuido} pedidos redistribuido)',
            '6 -Detalhamento': pedidos.to_dict(orient='records')

        }
        return pd.DataFrame([dados])





