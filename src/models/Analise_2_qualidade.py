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

        ordemProd = OrdemProd.OrdemProd(self.codEmpresa,'',self.data_inicio, self.data_final)
        ordemProd_baixadas = ordemProd.ops_baixas_csw()

        ordemProd_faccionistas = ordemProd.ops_baixas_faccionista_csw()
        ordemProd_faccionistas['nomeOrigem'] = 'COSTURA'

        tags = pd.merge(tags, ordemProd_faccionistas, on=['OPpai','nomeOrigem'], how='left')

        tags.fillna('-',inplace=True)

        TotalPCsBaixadas = ordemProd_baixadas['qtdMovto'].astype(int).sum()

        data = {
            '1- Peças com Motivo de 2Qual.': TotalPecas,
            '2- Total Peças Baixadas periodo': TotalPCsBaixadas,
            '4- Detalhamento ': tags.to_dict(orient='records')
        }
        return pd.DataFrame([data])


    def motivos_agrupo_periodo(self):
        '''Metodo publico que retorna os motivos de defeitos agrupaodos , de acordo com um determinado periodo; '''

        data = self.get_busca_defeitos_apontados()
        data = data.groupby(['motivo2Qualidade']).agg({
            'qtde':'sum'
        }).reset_index()

        return data







