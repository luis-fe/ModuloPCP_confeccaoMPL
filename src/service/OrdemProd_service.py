import numpy as np
import pandas as pd

from src.models import OrdemProd_Csw





class OrdemProd_service():
    def __init__(self, codEmpresa):

        self.codEmpresa = codEmpresa
        self.ordemProd_csw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa)


    def ordemProd_requisicao_gerada(self):
        '''Metodo publico que busca as Ordem de Producao com requisicao em aberto '''

        ordemProd_aberto = self.ordemProd_csw.ordem_Prod_em_aberto()

        self.ordemProd_csw.codFase = '409'
        ordemProd_pos_fase = self.ordemProd_csw.ops_emAberto_movimentacao_fase()
        ordemProd_pos_fase['passou_separacao'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase, on = 'numeroOP',how='left')

        self.ordemProd_csw.codFase = '428'
        ordemProd_pos_fase2 = self.ordemProd_csw.ops_emAberto_movimentacao_fase()

        ordemProd_pos_fase2['passou_costura'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase2, on = 'numeroOP',how='left')

        ordemProd_aberto.fillna('-',inplace=True)
        # Lógica para criar a coluna situacaoOP
        ordemProd_aberto['situacaoOP'] = np.where(
            (ordemProd_aberto['passou_costura'] == '-') &
            (ordemProd_aberto['passou_separacao'] == 'sim'),
            'Em Operacao Almoxarifado',  # Texto caso a condição seja verdadeira
            '-'  # Texto caso seja falsa (ou mantenha o padrão)
        )

        df_requisicoes = self.ordemProd_csw.requisicaoes_ops_em_aberto()
        df_requisicoes.fillna('-',inplace=True)

        # Agrupamento compatível com Pandas antigo (Python 3.6)
        requisicoes_agrupadas = (
            df_requisicoes.groupby('numeroOP')
            .apply(lambda x: x.to_dict('records'))
            .reset_index(name='requisicoes')
        )

        # Merge com o DataFrame principal
        ordemProd_aberto = pd.merge(ordemProd_aberto, requisicoes_agrupadas, on='numeroOP', how='left')

        # Tratamento para garantir que seja uma lista (mesmo que vazia) e não NaN
        ordemProd_aberto['requisicoes'] = ordemProd_aberto['requisicoes'].apply(
            lambda d: d if isinstance(d, list) else [])

        return ordemProd_aberto






