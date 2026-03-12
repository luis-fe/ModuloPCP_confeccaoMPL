from datetime import datetime

import pandas as pd
import pytz

from src.models import Endereco_aviamento


class Conferencia_itens_separados():
    '''Classe de servico responsavel por gerenciar a tela de COnferecnia'''


    def __init__(self, codEmpresa :str = '1', codMaterial: str = '', numeroOP: str = '', matricula: str = '' ):

        self.codEmpresa = codEmpresa
        self.codMaterial = codMaterial
        self.numeroOP = numeroOP


        self.matricula = matricula


    def carregar_ordens_para_conferencia(self):
        '''Metodo que carrega os itens disponivel para conferencia'''


        consulta = Endereco_aviamento.Endereco_aviamento().get_ops_paraConferir()
        consulta.fillna('-',inplace=True)

        return consulta


    def carregar_itens_para_conferir(self):
        '''Metodo que explode os itens a serem conferidos'''
        consulta = Endereco_aviamento.Endereco_aviamento('','','','','','',0,0,self.numeroOP).get_carregar_itens_conferir()
        consulta.fillna('-',inplace=True)

        return consulta

    def inserir_conferencia(self):
        '''Metodo responsavel pela insercao do qrCode conferido'''
        consulta = Endereco_aviamento.Endereco_aviamento('','','','',self.codMaterial,'',0,0,self.numeroOP).update_conferencia_item_op()

        return pd.DataFrame([{'status':True, 'Mensagem':'Item conferido com sucesso'}])


    def estornar_conferencia(self):
        '''Metodo responsavel pelo estorno do qrCode conferido'''


    def finalizar_conferencia(self):
        '''Metodo que finaliza a conferencia da OP_requisicao_aviamento'''

        consulta = Endereco_aviamento.Endereco_aviamento('','','','','',self.__obter_data_hora() ,0,0,self.numeroOP,self.matricula).inserir_finalizacao_confencia()

        return pd.DataFrame([{'status':True, 'Mensagem':'Finalizado com sucesso'}])

    def finalizar_conferencia_comPendencia(self):
        '''Metodo que finaliza a conferencia da OP_requisicao_aviamento'''

        consulta = Endereco_aviamento.Endereco_aviamento('','','','','',self.__obter_data_hora() ,0,0,self.numeroOP,self.matricula).inserir_finalizacao_confencia('pendente')

        return pd.DataFrame([{'status':True, 'Mensagem':'Finalizado com sucesso'}])



    def __obter_data_hora(self):
        """Metodo privado para obter a dataHora do Sistema Operacional em fuso-br """
        fuso_horario = pytz.timezone('America/Sao_Paulo')
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora
