import numpy as np
import pandas as pd
from src.models import OrdemProd_Csw, UsuarioRequisicao, DashboardTV
from src.connection import ConexaoPostgre



class Automacao ():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa
        self.ordemProd_csw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa)


    def buscar_informacao_aviamentos_disponiveis_CSW(self):
        '''Metodo que busca e processa as informacoes dos aviamentos disponiveis para separacao,
         e alimenta o Postgree no disparo da Automacao'''

        # 1 - Buscando as informacoes das Ops habilitadas para separacao

        ordemProd_aberto = self.ordemProd_csw.ordem_Prod_em_aberto()

        self.ordemProd_csw.codFase = '409'
        ordemProd_pos_fase = self.ordemProd_csw.ops_emAberto_movimentacao_fase()
        ordemProd_pos_fase['passou_separacao'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase, on='numeroOP', how='left')

        self.ordemProd_csw.codFase = '428'
        ordemProd_pos_fase2 = self.ordemProd_csw.ops_emAberto_movimentacao_fase()

        ordemProd_pos_fase2['passou_costura'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase2, on='numeroOP', how='left')

        ordemProd_aberto.fillna('-', inplace=True)

        # Lógica para criar a coluna situacaoOP
        ordemProd_aberto['situacaoOP'] = np.where(
            (ordemProd_aberto['passou_costura'] == '-') &
            (ordemProd_aberto['passou_separacao'] == 'sim'),
            'Em Operacao Almoxarifado',
            '-'
        )

        ordemProd_aberto = ordemProd_aberto[ordemProd_aberto['situacaoOP'] == 'Em Operacao Almoxarifado'].reset_index(
            drop=True)

        # 1. Remove as duplicatas e valores nulos (opcional, mas recomendado)
        valores_unicos = ordemProd_aberto['numeroOP'].drop_duplicates().dropna()

        # 2. Formata como uma string separada por vírgula e aspas
        clausula_in = f"""IN ({', '.join([f"'{val}'" for val in valores_unicos])})"""

        requisicoes = self.ordemProd_csw.explodir_requisicao_opS(clausula_in)

        print(clausula_in)

        ordemProd_aberto = pd.merge(ordemProd_aberto, requisicoes, on ='numeroOP' , how='left')

        ordemProd_aberto.fillna('-',inplace=True)

        ConexaoPostgre.Funcao_InserirPCPMatriz(ordemProd_aberto, ordemProd_aberto['numeroOP'].size, 'AviamentosDisponiveis', 'replace')





