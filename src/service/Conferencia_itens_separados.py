from src.models import Endereco_aviamento


class Conferencia_itens_separados():
    '''Classe de servico responsavel por gerenciar a tela de COnferecnia'''


    def __init__(self, codEmpresa :str = '1', codMaterial: str = '' ):

        self.codEmpresa = codEmpresa
        self.codMaterial = codMaterial

    def carregar_itens_para_conferencia(self):
        '''Metodo que carrega os itens disponivel para conferencia'''

        consulta = Endereco_aviamento.Endereco_aviamento().get_item_qtd_op_CONFERENCIA()

    def inserir_conferencia(self):
        '''Metodo responsavel pela insercao do qrCode conferido'''


    def estornar_conferencia(self):
        '''Metodo responsavel pelo estorno do qrCode conferido'''


    def finalizar_conferencia(self):
        '''Metodo que finaliza a conferencia da OP_requisicao_aviamento'''
