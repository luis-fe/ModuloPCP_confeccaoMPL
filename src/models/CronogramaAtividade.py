
from src.connection import ConexaoPostgre
import pandas as pd

class CronogramaAtividades():
    '''Classe que interage com o planejamento de cronograma'''

    def __init__(self, atividade = '', dataPrevInicio = '',
                 dataFinalPrev = '', responsavel = '' ):

        self.atividade = atividade
        self.dataPrevInicio = dataPrevInicio
        self.dataFinalPrev = dataFinalPrev
        self.responsavel = responsavel


    def get_cronograma(self):
        '''Metodo que obtem o cronograma previsto de atividade'''

        conn = ConexaoPostgre.conexaoEngine()

        sql = """
        Select * from "DashbordTV"."AcompanhamentoAtividades"
        """

        consulta = pd.read_sql(sql, conn)
        consulta['dataInicio'] = pd.to_datetime(consulta['dataInicio'], utc=True)
        consulta['dataFinal'] = pd.to_datetime(consulta['dataFinal'], utc=True)

        # Formatar para o padrão "dd-mm-aaaa"
        consulta['dataInicio'] = consulta['dataInicio'].dt.strftime('%d-%m-%Y')
        consulta['dataFinal'] = consulta['dataFinal'].dt.strftime('%d-%m-%Y')

        return consulta