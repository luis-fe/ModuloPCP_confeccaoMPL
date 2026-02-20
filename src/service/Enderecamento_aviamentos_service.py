import pandas as pd

from src.models import Endereco_aviamento, Produtos_CSW

class Enderecamento_aviamento():

    def __init__(self, codEmpresa = '1', rua = '', quadra = '', posicao = ''):

        self.codEmpresa = codEmpresa
        self.rua = rua
        self.quadra = quadra
        self.posicao = posicao

        self.endereco = f'{self.rua}-{self.quadra}-{self.posicao}'


    def fila_itens_enderecar(self):
        '''Metodo que obtem do ERP a Fila de Itens a serem enderecados '''

        fila = Produtos_CSW.Produtos_CSW(self.codEmpresa).estoqueNat_aviamentos()

        return fila

    def get_enderecos(self):
        '''Metodo para obter os enderecos cadastrados '''

        consulta = Endereco_aviamento.Endereco_aviamento().get_enderecos()

        return consulta

    def inserir_endereco(self):
        '''Metodo para inserir os enderecos '''
        enderecamento = Endereco_aviamento.Endereco_aviamento(self.endereco,self.rua, self.posicao, self.quadra)
        verifica = enderecamento.consulta_endereco_individual()

        if verifica.empty:

            enderecamento.insert_endereco()

            return pd.DataFrame([{'Mensagem':'Endereco Incluido com sucesso','status':True}])

        else:

            return pd.DataFrame([{'Mensagem':'Endereco ja existe ','status':False}])
