import pandas as pd
import gc
import fastparquet as fp
from src.connection import ConexaoERP
from src.connection import ConexaoPostgre
from src.models import ServicoAutomacao
from datetime import datetime
import pytz

class Pedido_venda():


    def __init__(self, codempresa='1', intervalo_automacao=3600):
        self.codempresa = codempresa
        self.intervalo_automacao = intervalo_automacao

    def incrementarPedidos(self):


        self.servicoAutomacao = ServicoAutomacao.ServicoAutomacao('006','Dados_CSW_Pedidos')
        self.ultima_atualizacao = self.servicoAutomacao.obtentendo_intervalo_atualizacao_servico()

        if self.ultima_atualizacao > self.intervalo_automacao:

            self.servicoAutomacao.inserindo_automacao(self.__obter_data_hora())




            sqlcswPedidosProdutos = """
            SELECT top 1500000 
                codItem as seqCodItem, 
                p.codPedido, 
                p.codProduto , 
                p.qtdePedida ,  
                p.qtdeFaturada, 
                p.qtdeCancelada  
            FROM 
                ped.PedidoItemGrade p
            WHERE 
                p.codEmpresa = 1 
                and p.codProduto  not like '8601000%' 
                and p.codProduto  not like '83060062%'  
                and p.codProduto  not like '8306000%' 
                and p.codProduto not like '8302003%' 
                and p.codProduto not like '8306003%' 
                and p.codProduto not like '8306006%' 
                and p.codProduto not like '8306007%'
            order by codPedido desc
            """


            sqlcswPedidosProdutos4 = """
            SELECT top 1000000 
                codItem as seqCodItem, 
                p.codPedido, 
                p.codProduto , 
                p.qtdePedida ,  
                p.qtdeFaturada, 
                p.qtdeCancelada  
            FROM 
                ped.PedidoItemGrade p
            WHERE 
                p.codEmpresa = 4
                and p.codProduto  not like '8601000%' 
                and p.codProduto  not like '83060062%'  
                and p.codProduto  not like '8306000%' 
                and p.codProduto not like '8302003%' 
                and p.codProduto not like '8306003%' 
                and p.codProduto not like '8306006%' 
                and p.codProduto not like '8306007%'
            order by codPedido desc
            """




            sqlcswValordosProdutos = """
            select top 450000 
                item.codPedido, 
                item.CodItem as seqCodItem, 
                item.precoUnitario, item.tipoDesconto, item.descontoItem, 
                case when tipoDesconto = 1 then ( (item.qtdePedida * item.precoUnitario) - item.descontoItem)/item.qtdePedida when item.tipoDesconto = 0 then (item.precoUnitario * (1-(item.descontoItem/100))) else item.precoUnitario end  PrecoLiquido 
            from 
                ped.PedidoItem as item 
            WHERE 
                item.codEmpresa = 1 
            order by 
                item.codPedido desc """


            sqlcswValordosProdutos4 = """
            select top 450000 
                item.codPedido, 
                item.CodItem as seqCodItem, 
                item.precoUnitario, item.tipoDesconto, item.descontoItem, 
                case when tipoDesconto = 1 then ( (item.qtdePedida * item.precoUnitario) - item.descontoItem)/item.qtdePedida when item.tipoDesconto = 0 then (item.precoUnitario * (1-(item.descontoItem/100))) else item.precoUnitario end  PrecoLiquido 
            from 
                ped.PedidoItem as item 
            WHERE 
                item.codEmpresa = 4 
            order by 
                item.codPedido desc """



            sqlcswSugestoesPedidos = """
            SELECT 
                p.codPedido , 
                p.produto as codProduto , 
                p.qtdeSugerida , 
                p.qtdePecasConf,
                case when 
                    (situacaoSugestao = 2 and dataHoraListagem>0) then 'Sugerido(Em Conferencia)' 
                    WHEN situacaoSugestao = 0 then 'Sugerido(Gerado)' 
                    WHEN situacaoSugestao = 2 then 'Sugerido(A listar)' 
                    else '' end StatusSugestao
            FROM 
                ped.SugestaoPedItem p
            inner join 
                ped.SugestaoPed c 
                on c.codEmpresa = p.codEmpresa 
                and c.codPedido = p.codPedido 
                and c.codSequencia = p.codSequencia 
            WHERE 
                p.codEmpresa in (1, 4) """

            sqlcswCapPedidos = """
                 SELECT top 500000 
                    p.codPedido , 
                    p.codTipoNota, 
                    p.dataemissao, 
                    p.dataPrevFat, 
                    p.situacao as situacaoPedido ,
                    (select c.nome from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeCliente,
                    (select c.nomeEstado from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeEstado ,
                    (select r.nome from fat.Representante  r WHERE r.codempresa = 1 and r.codRepresent = p.codRepresentante) as nomeRepresentante 
                FROM 
                    ped.Pedido p
                WHERE 
                    p.codEmpresa = 1
                order by 
                    p.codPedido desc
                """


            sqlcswCapPedidos4 = """
                 SELECT top 30000 
                    p.codPedido , 
                    p.codTipoNota, 
                    p.dataemissao, 
                    p.dataPrevFat, 
                    p.situacao as situacaoPedido ,
                    (select c.nome from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeCliente,
                    (select c.nomeEstado from fat.Cliente c WHERE c.codempresa = 1 and c.codCliente = p.codCliente) as nomeEstado ,
                    (select r.nome from fat.Representante  r WHERE r.codempresa = 1 and r.codRepresent = p.codRepresentante) as nomeRepresentante 
                FROM 
                    ped.Pedido p
                WHERE 
                    p.codEmpresa = 4
                order by 
                    p.codPedido desc
                """

            with ConexaoERP.ConexaoInternoMPL() as conn:
                with conn.cursor() as cursor_csw:
                    # Executa a primeira consulta e armazena os resultados
                    cursor_csw.execute(sqlcswPedidosProdutos)
                    colunas = [desc[0] for desc in cursor_csw.description]
                    rows = cursor_csw.fetchall()
                    pedidos = pd.DataFrame(rows, columns=colunas)
                    del rows, colunas
                    print('rodou pedidos1')

                    # Executa a primeira consulta e armazena os resultados
                    cursor_csw.execute(sqlcswPedidosProdutos4)
                    colunas = [desc[0] for desc in cursor_csw.description]
                    rows = cursor_csw.fetchall()
                    pedidos4 = pd.DataFrame(rows, columns=colunas)
                    del rows, colunas
                    print('rodou pedidos4')

                    pedidos = pd.concat([pedidos, pedidos4])
                    print('rodou concatenou pedidos item grade empresa 1 e 4')

                    # Executa a segunda consulta e armazena os resultados
                    cursor_csw.execute(sqlcswValordosProdutos)
                    colunas2 = [desc[0] for desc in cursor_csw.description]
                    rows2 = cursor_csw.fetchall()
                    pedidosValores = pd.DataFrame(rows2, columns=colunas2)
                    print('rodou pedidosValores')


                    # Executa a segunda consulta e armazena os resultados
                    cursor_csw.execute(sqlcswValordosProdutos4)
                    colunas2 = [desc[0] for desc in cursor_csw.description]
                    rows2 = cursor_csw.fetchall()
                    pedidosValores4 = pd.DataFrame(rows2, columns=colunas2)
                    print('rodou pedidosValores4')

                    pedidosValores = pd.concat([pedidosValores, pedidosValores4])
                    print('rodou concatenou pedidosValores4   empresa 1 e 4')


                    pedidos = pd.merge(pedidos, pedidosValores, on=['codPedido', 'seqCodItem'], how='left')
                    print('o merge deu certo entre pedidos e valores')


                    del pedidosValores, rows2

                    # Executa a terceira consulta e armazena os resultados
                    cursor_csw.execute(sqlcswSugestoesPedidos)
                    colunas3 = [desc[0] for desc in cursor_csw.description]
                    rows3 = cursor_csw.fetchall()
                    sugestoes = pd.DataFrame(rows3, columns=colunas3)
                    pedidos = pd.merge(pedidos, sugestoes, on=['codPedido', 'codProduto'], how='left')
                    del sugestoes, rows3
                    print('rodou sqlcswSugestoesPedidos')


                    # Executa a quarta consulta e armazena os resultados
                    cursor_csw.execute(sqlcswCapPedidos)  # Verifique se a consulta é correta
                    colunas4 = [desc[0] for desc in cursor_csw.description]
                    rows4 = cursor_csw.fetchall()
                    capaPedido = pd.DataFrame(rows4, columns=colunas4)
                    print('rodou sqlcswCapPedidos')


                    cursor_csw.execute(sqlcswCapPedidos4)  # Verifique se a consulta é correta
                    colunas4 = [desc[0] for desc in cursor_csw.description]
                    rows4 = cursor_csw.fetchall()
                    capaPedido4 = pd.DataFrame(rows4, columns=colunas4)
                    print('rodou sqlcswCapPedidos4')

                    capaPedido = pd.concat([capaPedido, capaPedido4])
                    print('rodou o cancatenado da capa emp 1 e 4')



                    pedidos = pd.merge(pedidos, capaPedido, on='codPedido', how='left')
                    print('rodou o mergem final')

                    # Limpeza de memória
                    del rows4, capaPedido
                    gc.collect()

                #etapa1 = controle.salvarStatus_Etapa1(self.rotina, 'automacao', self.dataInicio, 'from ped.pedidositemgrade')

                # Usando ~ para negar a condição do isin. Evita múltiplas avaliações booleanas.
                notas_para_excluir = ['38', '239', '223']  # Deixe como int ou float, dependendo de como vem do banco
                pedidos = pedidos[~pedidos['codTipoNota'].isin(notas_para_excluir)]

                fp.write('/app/dados/pedidos.parquet', pedidos)

                #etapa2 = controle.salvarStatus_Etapa2(self.rotina, 'automacao', etapa1, 'Geracao do arquivo parquet no servidor origem')
                self.servicoAutomacao.update_controle_automacao('Finalizado', self.__obter_data_hora())

                return pedidos

    def __obter_data_hora(self):
                """Metodo privado para obter a dataHora do Sistema Operacional em fuso-br """
                fuso_horario = pytz.timezone('America/Sao_Paulo')
                agora = datetime.now(fuso_horario)
                agora = agora.strftime('%Y-%m-%d %H:%M:%S')
                return agora