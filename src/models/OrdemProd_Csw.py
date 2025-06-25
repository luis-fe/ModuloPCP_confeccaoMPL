import gc

import pandas as pd
from src.connection import ConexaoERP



class OrdemProd_Csw():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa

    def get_fasesCsw(self):
        '''Metodo que retorna as fases do ERP CSW'''

        sql = """
        SELECT
            convert(varchar(5),codFase) as codFaseAtual,
            nome as nomeFase
        FROM
            tcp.FasesProducao
        where
            codempresa = 1 
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            del rows
            return consulta

