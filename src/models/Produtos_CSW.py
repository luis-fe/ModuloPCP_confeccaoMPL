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
