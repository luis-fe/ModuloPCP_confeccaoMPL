import pandas as pd
from src.connection import ConexaoPostgre

class DashboardTV():
        ''''Classe responsavel pelo gerenciamento do Dashboard da TV '''

        def __init__(self, codEmpresa = '', codAno = ''):

            self.codEmpresa = str(codEmpresa)
            self.codAno = str(codAno)


        def get_metas_cadastradas_ano_empresa(self):
            '''Metodo que obtem as metas cadastradas para um determinado ano '''

            sql = """
            select
                *
            from
                "PCP"."DashbordTV".metas m
            where 
                m.ano = %s 
                and m.empresa = %s
            order by mes 
            """
            conn = ConexaoPostgre.conexaoEngine()
            consulta = pd.read_sql(sql,conn, params=(self.codAno, self.codEmpresa))

            return consulta



