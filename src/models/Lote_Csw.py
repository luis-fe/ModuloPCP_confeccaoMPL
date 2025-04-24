import gc
import pandas as pd
from src.conection import ConexaoERP

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


