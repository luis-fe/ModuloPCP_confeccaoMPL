import pandas as pd

from src.models import Endereco_aviamento


class Conferencia_itens_separados():
    '''Classe de servico responsavel por gerenciar a tela de COnferecnia'''


    def __init__(self, codEmpresa :str = '1', codMaterial: str = '', numeroOP: str = '' ):

        self.codEmpresa = codEmpresa
        self.codMaterial = codMaterial
        self.numeroOP = numeroOP

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
