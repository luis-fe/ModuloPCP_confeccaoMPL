import pandas as pd
from src.connection import ConexaoPostgre
from src.models import OrdemProd
import numpy as np


class Analise_2_qualidade():
    '''Classe para analise de 2 Qualidade '''

    def __init__(self, codEmpresa = '1', data_inicio = '', data_final = ''):

        self.codEmpresa = codEmpresa
        self.data_inicio = data_inicio
        self.data_final = data_final

    def get_busca_defeitos_apontados(self, textoAvancao=''):
        '''Metodo publico que contabiliza a 2ª Qualidade no periodo'''

        sql = """
        SELECT
            *
        FROM
            "PCP".pcp.tags_defeitos_csw tdc 
        WHERE 
            data_receb >= %s
            AND data_receb <= %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.data_inicio, self.data_final))
        ordemProd = OrdemProd.OrdemProd(self.codEmpresa,'',self.data_inicio, self.data_final)

        ordemProd_faccionistas = ordemProd.ops_baixas_faccionista_csw()
        ordemProd_faccionistas['nomeOrigem'] = 'COSTURA'

        consulta = pd.merge(consulta, ordemProd_faccionistas, on=['OPpai', 'nomeOrigem'], how='left')
        consulta.fillna('-',inplace=True)
        consulta['fornencedorPreferencial'] = np.where(consulta['nomeOrigem'] != 'LABORATORIO', '-', consulta['fornencedorPreferencial'])
        consulta['nomeItem'] = np.where(consulta['nomeOrigem'] != 'LABORATORIO', '-', consulta['nomeItem'])
        consulta['nomeItem'] = consulta['nomeItem'].str.replace(' SEM ACESSORIO', '', regex=False)

        # Cria a coluna de busca concatenando os campos relevantes
        consulta['textoAvançado'] = (
                consulta['fornencedorPreferencial'].astype(str)
                + ' ' + consulta['nome'].astype(str)
                + ' ' + consulta['nomeItem'].astype(str)
                + ' ' + consulta['nomeOrigem'].astype(str)
                + ' ' + consulta['nomeFaccicionista'].astype(str)
        )


        # Aplica o filtro tipo "LIKE %texto%"
        if textoAvancao.strip() != '':
            consulta = consulta[consulta['textoAvançado'].str.contains(textoAvancao, case=False, na=False)]


        return consulta

    def dashboard_TOTAL_tags_2_qualidade_periodo(self):
        '''Metodo publico que retorna o total de tags de 2 qualidade '''

        tags = self.get_busca_defeitos_apontados()



        TotalPecas = tags['qtd'].sum()

        ordemProd = OrdemProd.OrdemProd(self.codEmpresa,'',self.data_inicio, self.data_final)
        ordemProd_baixadas = ordemProd.ops_baixas_csw()



        tags.fillna('-',inplace=True)

        TotalPCsBaixadas = ordemProd_baixadas['qtdMovto'].astype(int).sum()

        data = {
            '1- Peças com Motivo de 2Qual.': TotalPecas,
            '2- Total Peças Baixadas periodo': TotalPCsBaixadas,
            '4- Detalhamento ': tags.to_dict(orient='records')
        }
        return pd.DataFrame([data])

    def motivos_agrupo_periodo(self, textoAvancado = ''):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados(textoAvancado)
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)

        data = (
            data.groupby(['motivo2Qualidade'], as_index=False)
            .agg({'qtd': 'sum','nome':'first',"nomeOrigem":'first'})
        )

        data['motivo2Qualidade'] = data['motivo2Qualidade']+'-'+data['nome']+' ('+data['nomeOrigem']+')'

        data = data.sort_values(by=['qtd'], ascending=False)

        return data


    def defeitos_faccionista_agrupo_periodo(self,textoAvancado = ''):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados(textoAvancado)
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)
        data = data[data['nomeOrigem']=='COSTURA'].reset_index()



        data = (
            data.groupby(['nomeFaccicionista'], as_index=False)
            .agg({'qtd': 'sum'})
        )


        data = data.sort_values(by=['qtd'], ascending=False)

        return data


    def defeitos_detalhado_periodo(self,textoAvancado = ''):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados(textoAvancado)
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)


        data = data.sort_values(by=['qtd'], ascending=False)

        data['fornencedorPreferencial'] = np.where(data['nomeOrigem'] != 'LABORATORIO', '-', data['fornencedorPreferencial'])
        data['fornencedorPreferencial'] = data['fornencedorPreferencial'].str.replace('LTDA', '', regex=False)
        data['nome'] = data['nome'].str.replace('FORA DE ESPECIFICACAO', 'FORA/ESPEC.', regex=False)
        data['nomeFaccicionista'] = data['nomeFaccicionista'].str.replace('LTDA', '', regex=False)

        return data




    def defeitos_Origem_agrupo_periodo(self,textoAvancado = ''):
        """Método público que retorna as ORGIEM de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados(textoAvancado)

        data = (
            data.groupby(['nomeOrigem'], as_index=False)
            .agg({'qtd': 'sum'})
        )


        data = data.sort_values(by=['qtd'], ascending=False)

        return data


    def defeitos_fornecedor_agrupo_periodo(self,textoAvancado = ''):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados(textoAvancado)
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)
        data = data[data['nomeOrigem']=='LABORATORIO'].reset_index()


        data = (
            data.groupby(['fornencedorPreferencial'], as_index=False)
            .agg({'qtd': 'sum'})
        )

        data = data[data['fornencedorPreferencial']!='-'].reset_index()
        data = data.sort_values(by=['qtd'], ascending=False)

        return data



    def defeitos_fornecedor_base_agrupo_periodo(self,textoAvancado = ''):
        """Método público que retorna os motivos de defeitos agrupados de acordo com um determinado período."""

        data = self.get_busca_defeitos_apontados(textoAvancado)
        data['motivo2Qualidade'] = data['motivo2Qualidade'].astype(str)
        data = data[data['nomeOrigem']=='LABORATORIO'].reset_index()


        data = (
            data.groupby(['nomeItem'], as_index=False)
            .agg({'qtd': 'sum'})
        )

        data = data[data['nomeItem']!='-'].reset_index()
        data = data.sort_values(by=['qtd'], ascending=False)

        return data









