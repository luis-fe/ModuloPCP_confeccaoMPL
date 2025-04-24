import pandas as pd

from src.conection import ConexaoPostgre


class Plano_Lote():
    '''Classe utilizada para interacao de Plano x lotes de producao '''
    def __init__(self, codEmpresa = None):

        self.codEmpresa = codEmpresa


    def consultaVinculoABC_Plano(self):
        '''Metodo que consulta a estrutura ABC vinculado ao plano '''

        sql = """
            select 
                * 
            from 
                pcp."Plano" p 
            inner join 
                pcp."LoteporPlano"  lp 
                on lp.plano  = p.codigo 
            where p.codigo = %s
        """


        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        return consulta