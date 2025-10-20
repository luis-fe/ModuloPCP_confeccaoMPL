import pandas as pd
from src.connection import ConexaoPostgre
from src.models import OrdemProd


class Analise_2_qualidade():
    '''Classe para analise de 2 Qualidade '''

    def __init__(self, codEmpresa = '1', data_inicio = '', data_final = ''):

        self.codEmpresa = codEmpresa
        self.data_inicio = data_inicio
        self.data_final = data_final



    def get_busca_defeitos_apontados(self):
        '''Metodo publico que contabiliza a 2 Qualidade no periodo '''


        sql = """
        select
            *
        from
            "PCP".pcp.tags_defeitos_csw tdc 
        where 
            data_receb >= %s
            and data_receb <= %s
        """
        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn, params=(self.data_inicio, self.data_final))

        return consulta



    def dashboard_TOTAL_tags_2_qualidade_periodo(self):
        '''Metodo publico que retorna o total de tags de 2 qualidade '''

        tags = self.get_busca_defeitos_apontados()



        TotalPecas = tags['qtd'].sum()

        ordemProd = OrdemProd.OrdemProd(self.codEmpresa,'',self.data_inicio, self.data_final).ops_baixas_csw()

        TotalPCsBaixadas = ordemProd['qtdMovto'].astype(int).sum()

        data = {
            '1- Peças com Motivo de 2Qual.': TotalPecas,
            '2- Total Peças Baixadas periodo': TotalPCsBaixadas,
            '4- Detalhamento ': tags.to_dict(orient='records')
        }
        return pd.DataFrame([data])








