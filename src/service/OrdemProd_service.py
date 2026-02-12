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

        self.ordemProd_csw.codFase = '429'
        ordemProd_pos_fase2 = self.ordemProd_csw.ops_emAberto_movimentacao_fase()

        ordemProd_pos_fase2['passou_costura'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase2, on = 'numeroOP',how='left')

        ordemProd_aberto.fillna('-',inplace=True)

        return ordemProd_aberto
