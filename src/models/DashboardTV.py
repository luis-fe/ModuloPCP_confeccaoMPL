import pandas as pd
from src.connection import ConexaoPostgre

class DashboardTV():
        ''''Classe responsavel pelo gerenciamento do Dashboard da TV '''

        def __init__(self, codEmpresa = '', codAno = ''):

            self.codEmpresa = str(codEmpresa)
            self.codAno = str(codAno)


        def get_metas_cadastradas_ano_empresa(self):
            '''Metodo que obtem as metas cadastradas para um determinado ano '''

            dados = [
                { "mes": "01-Janeiro"},
                { "mes": "02-Fevereiro"},
                { "mes": "03-Março"},
                {"mes": "04-Abril"},
                { "mes": "05-Maio"},
                {"mes": "06-Junho"},
                {"mes": "07-Julho"},
                {"mes": "08-Agosto"},
                {"mes": "09-Setembro"},
                {"mes": "10-Outubro"},
                {"mes": "11-Novembro"},
                {"mes": "12-Dezembro"}
            ]

            # DataFrame somente com a coluna 'mes'
            mes = pd.DataFrame(dados)[["mes"]]


            sql = """
            select
                mes, meta
            from
                "PCP"."DashbordTV".metas m
            where 
                m.ano = %s 
                and m.empresa = %s
            order by mes 
            """
            conn = ConexaoPostgre.conexaoEngine()
            consulta = pd.read_sql(sql,conn, params=(self.codAno, self.codEmpresa))

            consulta = pd.merge(mes, consulta, on='mes', how='left')
            consulta.fillna('R$0,00',inplace=True)

            consulta["meta"] = consulta["meta"].apply(
                lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

            return consulta



