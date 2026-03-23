import pandas as pd
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

        leadTime_df['dataInicio'] = pd.to_datetime(leadTime_df['dataInicio'])
        leadTime_df['dataBaixa'] = pd.to_datetime(leadTime_df['dataBaixa'])

        leadTime_df['lead time realizado'] = (leadTime_df['dataBaixa'] - leadTime_df['dataInicio']).dt.days

        leadTime_df['dataInicio'] = leadTime_df['dataInicio'].dt.strftime('%Y-%m-%d')
        leadTime_df['dataBaixa'] = leadTime_df['dataBaixa'].dt.strftime('%Y-%m-%d')


        dados = {
            'Média Pondereda': True,
            'Detalhamento Lead Time Geral': leadTime_df.to_dict(orient='records')

        }
        return pd.DataFrame([dados])









