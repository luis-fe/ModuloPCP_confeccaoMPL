
from src.models import OrdemProd

class LeadTime ():


    def __init__(self, codEmpresa = '1', dataInico: str = '', dataFim : str = ''):

        self.codEmpresa = codEmpresa
        self.dataInicio = dataInico
        self.dataFim = dataFim


    def get_leadTime_global_periodo(self):

        '''Obter o Lead Time Global'''

        ordemProd = OrdemProd.OrdemProd(self.codEmpresa, '', self.dataInicio, self.dataFim)

        leadTime_df = ordemProd.get_leadTime_completo()

        # 1 - encontrando a ponderacao pela qtd da OP



