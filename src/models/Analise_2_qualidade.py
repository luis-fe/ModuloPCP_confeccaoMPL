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
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados()
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)

        data = (
            data.groupby(['motivo2Qualidade'], as_index=False)
            .agg({'qtd': 'sum','nome':'first',"nomeOrigem":'first'})
        )

        data['motivo2Qualidade'] = data['motivo2Qualidade']+'-'+data['nome']+' ('+data['nomeOrigem']+')'

        data = data.sort_values(by=['qtd'], ascending=False)

        return data


    def defeitos_faccionista_agrupo_periodo(self):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados()
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)
        data = data[data['nomeOrigem']=='COSTURA'].reset_index()
        ordemProd = OrdemProd.OrdemProd(self.codEmpresa,'',self.data_inicio, self.data_final)
        ordemProd_faccionistas = ordemProd.ops_baixas_faccionista_csw()
        data = pd.merge(data, ordemProd_faccionistas, on='OPpai', how='left')
        data.fillna('-',inplace=True)


        data = (
            data.groupby(['nomeFaccicionista'], as_index=False)
            .agg({'qtd': 'sum'})
        )


        data = data.sort_values(by=['qtd'], ascending=False)

        return data


    def defeitos_detalhado_periodo(self):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados()
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)
        ordemProd = OrdemProd.OrdemProd(self.codEmpresa,'',self.data_inicio, self.data_final)
        ordemProd_faccionistas = ordemProd.ops_baixas_faccionista_csw()
        ordemProd_faccionistas['nomeOrigem'] = 'COSTURA'

        data = pd.merge(data, ordemProd_faccionistas, on=['OPpai','nomeOrigem'], how='left')
        data.fillna('-',inplace=True)

        data = data.sort_values(by=['qtd'], ascending=False)

        return data




    def defeitos_Origem_agrupo_periodo(self):
        """Método público que retorna as ORGIEM de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados()

        data = (
            data.groupby(['nomeOrigem'], as_index=False)
            .agg({'qtd': 'sum'})
        )


        data = data.sort_values(by=['qtd'], ascending=False)

        return data


    def defeitos_fornecedor_agrupo_periodo(self):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados()
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)
        data = data[data['nomeOrigem']=='LABORATORIO'].reset_index()


        data = (
            data.groupby(['fornencedorPreferencial'], as_index=False)
            .agg({'qtd': 'sum'})
        )

        data = data[data['fornencedorPreferencial']!='-'].reset_index()
        data = data.sort_values(by=['qtd'], ascending=False)

        return data










