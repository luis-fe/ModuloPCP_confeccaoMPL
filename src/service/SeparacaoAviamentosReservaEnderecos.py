from src.models import Endereco_aviamento
import numpy as np
import pandas as pd

class Reserva_Enderecos():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa

    def carregar_tabela_reserva_enderecos(self):
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

        # 1 - Get das requisicoes em aberto
        consulta1 = endereco_aviamento.get_requisicao_itens()

        # 2 - Get do mapa de enderecos a nivel de item e etiqueta ordenado do maior para menor
        consulta2 = endereco_aviamento.get_itens_repostos_para_reserva()
        consulta2['ocorrencia_acumulada'] = consulta2.groupby(['codItem']).cumcount() + 1

        # 3 - Ciclo 1 : Primeiro merge
        consulta2 = consulta2[consulta2['ocorrencia_acumulada'] == 1].reset_index()
        consulta = pd.merge(consulta1, consulta2, on='codItem', how='left')
        consulta.fillna('',inplace=True)
        consulta = consulta[consulta['ocorrencia_acumulada'] == 1].reset_index()


        consulta['qtd'].fillna(0,inplace=True)

        # --- NOVA LÓGICA COM NUMPY ---
        consulta['endereco_reservado'] = np.where(
            consulta['qtdeRequisitada'] >= consulta['qtd'],  # Condição
            consulta['endereco'],  # Valor se Verdadeiro
            "Não Reposto"  # Valor se Falso
        )


        return consulta