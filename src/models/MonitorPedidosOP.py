import pandas as pd
from src.models.Pedidos_CSW import Pedidos_CSW
from src.models.Pedidos import Pedidos

class MonitorPedidosOP():
    '''Classe que gerencia o processo de Monitor de Pedidos'''

    def __init__(self, empresa, dataInicioVendas, dataFinalVendas, tipoDataEscolhida, dataInicioFat, dataFinalFat,
                 arrayRepresentantesExcluir='',
                 arrayEscolherRepresentante='', arrayescolhernomeCliente='', parametroClassificacao=None,
                 filtroDataEmissaoIni='', filtroDataEmissaoFim='',
                 analiseOPGarantia=None):

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
        self.pedidos_CSW = Pedidos_CSW(self.empresa, '', self.filtroDataEmissaoIni, self.filtroDataEmissaoFim, self.dataInicioVendas, self.dataFinalVendas, self.dataInicioFat, self.dataFinalFat, self.arrayTipoNota)
        self.faturamento = Pedidos(self.empresa)
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




            sku = self.faturamento.listagemPedidosSku('',self.dataInicioVendas, self.dataFinalVendas)
        else:
            sku = self.Monitor_nivelSkuPrev()
