import gc
import pandas as pd
from src.connection import ConexaoERP

class Lote_Csw():
    '''Classe utilizada para obter informacoes de Lote no ERP CSW'''

    def __init__(self,codLoteCsw = None, codEmpresa = '1'):
        self.codLoteCsw = codLoteCsw
        self.codEmpresa = codEmpresa

    def obterLotesCsw(self):
        """
        Get dos Lotes cadastrados no CSW para PREVISAO.
        """

        if self.codEmpresa == '1':
            sql = """ 
            SELECT 
                codLote, 
                descricao as nomeLote 
            FROM 
                tcl.Lote  l
            WHERE 
                l.descricao like '%PREV%' 
                and l.codEmpresa = 1 
                order by codLote desc 
                """
        else:
            sql = """ SELECT 
                        codLote, 
                        descricao as nomeLote 
                    FROM tcl.Lote  l
                    WHERE 
                    l.descricao like '%PREV%' and l.codEmpresa = 4 order by codLote desc """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return lotes


    def roteiroEng_CSW_peloLote(self):
        '''Metodo que busca do ERP as engenharias do lote e o seu roteiro de producao'''

        sql = """
            SELECT p.codEngenharia , p.codFase , p.nomeFase, p.seqProcesso  FROM tcp.ProcessosEngenharia p
        WHERE p.codEmpresa = 1 and p.codEngenharia like '%-0' and 
        p.codEngenharia in (select l.codEngenharia from tcl.LoteEngenharia l WHERE l.empresa =""" + str(
            self.codEmpresa) + """ and l.codlote in ( """ + self.codLoteCsw + """))"""

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                EngRoteiro = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        return EngRoteiro


    def consultarLoteEspecificoCsw(self):
        sql = """Select codLote, descricao as nomeLote from tcl.lote where codEmpresa= """ + str(
            self.codEmpresa) + """ and codLote =""" + "'" + self.codLoteCsw + "'"

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        nomeLote = lotes['nomeLote'][0]
        nomeLote = nomeLote[:2] + '-' + nomeLote

        return nomeLote


    def getLoteSeqTamanhoCsw(self):
        '''Metodo que busca no csw as informacoes da  tabela getLoteSeqTamanhoCsw que detalha a nivel de cor e tamanho os skus vinculados a um Lote Gerado'''

        # Pesquisando no ERP CSW o Lote vinculado
        sqlLotes = """
                select Empresa , t.codLote, codengenharia, t.codSeqTamanho , t.codSortimento , t.qtdePecasImplementadas as previsao FROM tcl.LoteSeqTamanho t
                WHERE t.Empresa = """ + self.codEmpresa + """and t.codLote in (""" + self.codLoteCsw + """) 
                """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlLotes)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)
        del rows
        gc.collect()

        return lotes


