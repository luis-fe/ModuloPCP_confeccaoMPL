import gc

import numpy as np
import pandas as pd
from src.connection import ConexaoERP

class Produtos_CSW():
    '''Classe utilizada para fazer buscas relativo aos Produtos cadastrados no ERP CSW'''

    def __init__(self, codEmpresa = '1' , codSku = None, ultimoItem = None, codNatureza = '5', codItemPai = '',codColecao=''):

        self.codSku = codSku
        self.ultimoItem = ultimoItem
        self.codNatureza = str(codNatureza)
        self.codEmpresa = codEmpresa
        self.codItemPai = codItemPai
        self.codColecao = codColecao


    def get_itensFilhos_Novos_CSW(self):
        '''Metodo que busca no csw os novos itens filhos que ainda nao foram atualizados no banco Postgre desse projeto '''

        sqlCSWItens = f"""
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
                    Cgi.Item2 i2 on i2.coditem = i.codigo and i2.Empresa = {self.codEmpresa}
                WHERE 
                    i.unidadeMedida in ('PC','KIT') 
                    and (i2.codItemPai like '1%' or i2.codItemPai like '2%'or i2.codItemPai like '3%'or i2.codItemPai like '5%'or i2.codItemPai like '6%' )
                    and i2.codItemPai > 0 and i.codigo > {str(self.ultimoItem)} """

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



    def obterColecaoCsw(self):
        '''Metodo utilizado para obter as Colecoes do ERP CSW DA CONSISTEM'''

        get = """
        SELECT
            c.codColecao ,
            c.nome
        FROM
            tcp.Colecoes c
        WHERE
            c.codEmpresa = 1
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(get)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta


    def obterNomeColecaoCSW(self):
        '''Metodo utilizado para obter um Determinado nome da Colecao  do ERP CSW DA CONSISTEM'''

        get = """
        SELECT
            c.codColecao ,
            c.nome
        FROM
            tcp.Colecoes c
        WHERE
            c.codEmpresa = 1 
            and 
            c.codColecao = """+str(self.codColecao)+""""""

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(get)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta['nome'][0]




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
                t.sequencia as codSeqTamanho, 
                t.descricao as tam
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


    def obter_engeharia_descricaoPai(self):
        '''Metodo que obtem os nomes das engenharias no csw '''


        sql = f"""
                Select codEngenharia, descricao from tcp.engenharia 
                where codEmpresa = 1
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



    def statusAFV(self):
        '''Metodo que consulta o status AFV dos skus '''

        sql = f"""
        SELECT
            b.Reduzido as codReduzido,
            'Bloqueado' as statusAFV
        FROM
            Asgo_Afv.EngenhariasBloqueadas b
        WHERE
            b.Empresa = {self.codEmpresa}
        union	
        SELECT
            b.Reduzido as codReduzido ,
            'Acompanhamento' as statusAFV
        FROM
            Asgo_Afv.EngenhariasAcompanhamento b
        WHERE
            b.Empresa = {self.codEmpresa}
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

        # prioridade: Bloqueado > Acompanhamento
        prioridade = {"Bloqueado": 1, "Acompanhamento": 2}

        # ordena pela prioridade e pega o primeiro status de cada codReduzido
        consulta = (
            consulta
            .sort_values(by="statusAFV", key=lambda x: x.map(prioridade))
            .drop_duplicates(subset="codReduzido", keep="first")
            .reset_index(drop=True)
        )

        return consulta

    def sqlEstoqueMP_nomes(self):
        '''Método que busca o estoque de aviamentos detalhado na nat 1, 3 , 2 ,10 juntamente com o Nome'''

        sql = f"""
                       SELECT
           	            d.codItem as CodComponente ,
           	            (select n.codnatureza||'-'||n.descricao from est.Natureza n WHERE n.codempresa = 1 and d.codNatureza = n.codnatureza)as natureza,
           	            i.nome,
           	            d.estoqueAtual
                       FROM
           	            est.DadosEstoque d
           	        join cgi.Item i on i.codigo = d.codItem 
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
                consumo = pd.DataFrame(rows, columns=colunas)

        return consumo

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


        sql = f"""
                 SELECT
                 	r.numOPConfec as OP,
        	        ri.codMaterial as CodComponente ,
        	        ri.nomeMaterial,
        	        ri.qtdeRequisitada as EmRequisicao
                FROM
        	        tcq.RequisicaoItem ri
                join 
                    tcq.Requisicao r on
        	        r.codEmpresa = {self.codEmpresa}
        	        and r.numero = ri.codRequisicao
                where
        	        ri.codEmpresa = {self.codEmpresa}
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


        sql = f"""
                SELECT
        		    i.codPedido as numero,
        		    i.codPedidoItem as seqitem,
        		    i.quantidade as qtAtendida
        	    FROM
        		    Est.NotaFiscalEntradaItens i
        	    WHERE
        		    i.codempresa = {self.codEmpresa} 
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


        sql = f"""
    SELECT
        i.nome
    FROM
        cgi.Item2 i2
    inner join cgi.Item i on
        i.codigo = i2.codItem
    WHERE
        i2.Empresa = {self.codEmpresa}
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


    def informacoesComponente(self):
        '''Metodo de informacao dos componentes '''


        sql = f"""
        SELECT
            q.codigo as CodComponente ,
            f.nomeFornecedor as fornencedorPreferencial,
            q.diasEntrega as LeadTime,
            q.qtdMinCom as LoteMin,
            q.qtdMultCom as loteMut,
            q.fatorConversao
            ,(SELECT i2.codeditado from cgi.Item2 i2 WHERE i2.Empresa = 2 and i2.codcor> 0 and i2.coditem = q.codigo) as codEditado
        FROM
            Cgi.FornecHomologados f
        right join 
            Cgi.DadosQualidadeFornecedor q on
            q.codEmpresa = f.codEmpresa
            and f.codItem = q.codigo
            and f.codFornecedor = q.codFornecedor
        WHERE
            f.codEmpresa = {self.codEmpresa}
            and f.fornecedorPreferencial = 1
            and q.referenciaPrincipal = 1
            and q.codigo > 18
                """

        sql2 = f"""
        SELECT
            f.CodItem as CodComponente ,
            f2.nomeFornecedor as novoNome
        FROM
            cgi.FornPreferItemFilho f
        inner join Cgi.FornecHomologados f2 on
            f.codItem = f2.codItem
            and f.codFornecedor = f2.codFornecedor
        WHERE
            f.Empresa = {self.codEmpresa}
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo = pd.DataFrame(rows, columns=colunas)


                cursor.execute(sql2)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consumo2 = pd.DataFrame(rows, columns=colunas)


        consumo['CodComponente'] = consumo['CodComponente'].astype(str)
        consumo2['CodComponente'] = consumo2['CodComponente'].astype(str)

        consumo = pd.merge(consumo, consumo2 , on='CodComponente', how='left')
        consumo['novoNome'].fillna('-',inplace=True)

        consumo['fornencedorPreferencial'] = np.where(
            consumo['novoNome'] == '-',
            consumo['fornencedorPreferencial'],
            consumo['novoNome']
        )

        consumo['fatorConversao'].fillna(1,inplace=True)
        consumo['LeadTime'].fillna(1,inplace=True)
        #consumo['CodComponente'] = consumo['CodComponente'].str.replace('.0','')
        print(consumo['CodComponente'])

        return consumo


    def obterColecaoItemPai(self):
        '''Metodo que obtem a colecao do item PAi  '''


        sql = f"""
        SELECT
            top 1 
            convert(varchar(10),e.codColecao) as colecao 
        FROM
            tcp.DadosGeraisEng  e
        WHERE
            e.codEmpresa = 1
            and e.codengenharia like '%{str(self.codItemPai)}%'
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



    def estoqueReduzido(self):

        consultasqlCsw = f"""
            select 
                dt.reduzido as codProduto, 
                SUM(dt.estoqueAtual) as estoqueAtual, 
                sum(estReservPedido) as estReservPedido 
            from
                (
                    select codItem as reduzido, 
                            estoqueAtual,estReservPedido  
                    from est.DadosEstoque where codEmpresa = {self.codEmpresa} and codNatureza = 5 and estoqueAtual > 0)dt
        group by dt.reduzido
         """
        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows

        return consulta

    def materiais_requisicao_OP_csw(self, dataBaixaInicial):
        '''Metodo publico que busca no csw o tecido principal baixado numa requisicao '''

        sql = f"""
        SELECT 
            r.numOPConfec, 
            codMaterial CodComponente,
            CONVERT(VARCHAR(6), r.numOPConfec) as OPpai,
            nomeMaterial, 
            r.dtEmissao,
            ri.codMaterialEdt,
            (select i.nome from cgi.item i where i.codigo = CONVERT(VARCHAR(9), ri.codMaterialEdt)) as nomeItem,
            ri.qtdeEntregue 
        FROM 
            tcq.Requisicao r
        INNER JOIN 
            tcq.RequisicaoItem ri 
            ON ri.codEmpresa = r.codEmpresa 
            AND ri.codRequisicao = r.numero
        WHERE 
            r.codEmpresa = 1
            AND r.codNatEstoque  = 2
            and r.seqRoteiro in (408, 409)
            AND r.dtBaixa  >= DATEADD(day, -80, TO_DATE('{dataBaixaInicial}', 'YYYY-MM-DD'))
        """
        print(sql)

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows


        consulta = consulta.sort_values(by=['qtdeEntregue'], ascending=False)
        consulta['ocorrencia'] = consulta.groupby('OPpai').cumcount() + 1
        consulta = consulta[consulta['ocorrencia']==1].reset_index()


        fornecedor = self.informacoesComponente()
        consulta = pd.merge(consulta, fornecedor, on='CodComponente', how='left')
        consulta.fillna('-', inplace=True)
        consulta.drop(['LeadTime','LoteMin','fatorConversao','index',
                       'ocorrencia','novoNome','codMaterialEdt','loteMut',"dtEmissao"], axis=1, inplace=True)


        return consulta




