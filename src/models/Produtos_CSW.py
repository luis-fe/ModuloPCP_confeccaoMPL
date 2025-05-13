import gc
import pandas as pd
from src.connection import ConexaoERP

class Produtos_CSW():
    '''Classe utilizada para fazer buscas relativo aos Produtos cadastrados no ERP CSW'''

    def __init__(self, codEmpresa = '1' , codSku = None, ultimoItem = None, codNatureza = '5'):

        self.codSku = codSku
        self.ultimoItem = ultimoItem
        self.codNatureza = str(codNatureza)
        self.codEmpresa = codEmpresa

    def get_itensFilhos_Novos_CSW(self):
        '''Metodo que busca no csw os novos itens filhos que ainda nao foram atualizados no banco Postgre desse projeto '''

        sqlCSWItens = """
                SELECT 
                    i.codigo , 
                    i.nome , 
                    i.unidadeMedida, 
                    i2.codItemPai, 
                    i2.codSortimento , 
                    i2.codSeqTamanho,
                    i2.codCor   
                FROM 
                    cgi.Item i
                JOIN 
                    Cgi.Item2 i2 on i2.coditem = i.codigo and i2.Empresa = 1
                WHERE 
                    i.unidadeMedida in ('PC','KIT') 
                    and (i2.codItemPai like '1%' or i2.codItemPai like '2%'or i2.codItemPai like '3%'or i2.codItemPai like '5%'or i2.codItemPai like '6%' )
                    and i2.codItemPai > 0 and i.codigo > """ + str(self.ultimoItem)

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlCSWItens)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()


        return consulta



    def estoqueNat(self):
        '''metodo que consulta o estoque da natureza 05 '''

        sql = f"""
    SELECT
        d.codItem as codReduzido,
        d.estoqueAtual
    FROM
        est.DadosEstoque d
    WHERE
        d.codEmpresa = {self.codEmpresa}
        and d.codNatureza = {self.codNatureza}
        and d.estoqueAtual > 0
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def get_tamanhos(self):
        '''Metodo que retorna os tamanhos do tcp do csw '''

        sql = """
        	SELECT
                t.sequencia as codSeqTamanho, t.descricao as tam
            FROM
                tcp.Tamanhos t
            WHERE
                t.codEmpresa = 1 
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        consulta['codSeqTamanho'] = consulta['codSeqTamanho'].astype(str)

        #print(f'consulta\n{consulta}')
        return consulta


    def statusAFV(self):
        '''Metodo que consulta o status AFV dos skus '''

        sql = """
        SELECT
            b.Reduzido as codReduzido,
            'Bloqueado' as statusAFV
        FROM
            Asgo_Afv.EngenhariasBloqueadas b
        WHERE
            b.Empresa = 1
        union	
        SELECT
            b.Reduzido as codReduzido ,
            'Acompanhamento' as statusAFV
        FROM
            Asgo_Afv.EngenhariasAcompanhamento b
        WHERE
            b.Empresa = 1
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def estMateriaPrima(self):
        '''Método que busca no CSW o estodque dos componentes de Materia Prima '''

        sql = f"""
                    SELECT
                        d.codItem as CodComponente ,
                        d.estoqueAtual
                    FROM
                        est.DadosEstoque d
                    WHERE
                        d.codEmpresa = {self.codEmpresa}
                        and d.codNatureza in (1, 3, 2,10)
                        and d.estoqueAtual > 0
                """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        return consulta



    def req_Materiais_aberto(self):
        '''Metodo que busca os componentes com requisicao em aberto no CSW'''


        sql = """
                 SELECT
                 	r.numOPConfec as OP,
        	        ri.codMaterial as CodComponente ,
        	        ri.nomeMaterial,
        	        ri.qtdeRequisitada as EmRequisicao
                FROM
        	        tcq.RequisicaoItem ri
                join 
                    tcq.Requisicao r on
        	        r.codEmpresa = 1
        	        and r.numero = ri.codRequisicao
                where
        	        ri.codEmpresa = 1
        	        and r.sitBaixa <0
                """



        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        return consulta



    def req_atendidoComprasParcial(self):
        '''Metodo que busca o que ja foi antendido parcial nas requisicoes de compras '''


        sql = """
                SELECT
        		    i.codPedido as numero,
        		    i.codPedidoItem as seqitem,
        		    i.quantidade as qtAtendida
        	    FROM
        		    Est.NotaFiscalEntradaItens i
        	    WHERE
        		    i.codempresa = 1 
        		    and i.codPedido >0 
        		    and codPedido in (select codpedido FROM sup.PedidoCompraItem p WHERE
        	        p.situacao in (0, 2))
                """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def pedidoComprasMP(self):
        '''Metodo que busca os pedidos de compras em aberto por componente '''

        sql = f"""
        		SELECT
        			s.codigo as numero,
        			'solicitacao' as tipo,
        			i2.codItem as CodComponente,
        			i.nome,
        			s.quantidade  as qtdPedida,
        			'-' as dataPrevisao,
        				s.situacao as sitSugestao,
        				itemSolicitacao as seqitem,
        				'1' as fatCon
        		FROM
        			sup.SolicitacaoComprasItem s
        		inner join Cgi.Item2 i2 on i2.Empresa = 1 and i2.codEditado = s.codItemEdt 
        		inner join cgi.item i on i.codigo = i2.coditem
        		WHERE
        			s.codEmpresa = {self.codEmpresa}
        			and s.situacao in (0, 2)
        	union
                SELECT
        	        p.codPedido as numero,
        	        'pedido' as tipo,
        	        p.codProduto as CodComponente,
        	        i.nome,
        	        p.quantidade as qtdPedida,
        	        p.dataPrevisao,
        	        p.situacao,
        	        p.itemPedido as  seqitem,
        	        p.fatCon
                from 
        	        sup.PedidoCompraItem p
        	    inner join cgi.item i on i.codigo = p.codProduto
                WHERE
        	        p.situacao in (0, 2)
        	        and p.codEmpresa = {self.codEmpresa}
                        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def pesquisarNomeMaterial(self, codigoMP = ''):
        '''Metodo que pesquisa o nome via codigoMaterial'''


        if codigoMP == '':
            codigoMP = str(self.codSku)
        else:
            codigoMP = codigoMP


        sql = """
    SELECT
        i.nome
    FROM
        cgi.Item2 i2
    inner join cgi.Item i on
        i.codigo = i2.codItem
    WHERE
        i2.Empresa = 1
        and i2.codEditado ='""" + codigoMP+"""'"""

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        if consulta.empty:
            return pd.DataFrame([{'status':False, 'nome':'produto nao existe'}])

        else:
            consulta['status'] = True
            return consulta

