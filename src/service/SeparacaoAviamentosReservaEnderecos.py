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
        consulta.fillna('', inplace=True)

        # Convertendo para numérico para garantir que os cálculos funcionem perfeitamente
        # (o fillna('') acima transforma os nulos em string, o que daria erro na subtração)
        consulta['qtd'] = pd.to_numeric(consulta['qtd'], errors='coerce').fillna(0)
        consulta['qtdeRequisitada'] = pd.to_numeric(consulta['qtdeRequisitada'], errors='coerce').fillna(0)

        # --- NOVA LÓGICA COM NUMPY ---
        consulta['endereco_reservado'] = np.where(
            consulta['qtdeRequisitada'] >= consulta['qtd'],  # Condição
            consulta['endereco'],  # Valor se Verdadeiro
            "Não Reposto"  # Valor se Falso
        )

        # --- LÓGICA DE CRIAÇÃO DO SALDO NOVO ---
        # 1. Cria a máscara booleana para identificar onde falta estoque
        mask_saldo = consulta['qtdeRequisitada'] > consulta['qtd']

        if mask_saldo.any():
            # 2. Faz uma cópia dessas linhas específicas
            linhas_saldo = consulta[mask_saldo].copy()

            # 3. Calcula o 'saldo novo' e atualiza a quantidade da nova linha
            linhas_saldo['qtdeRequisitada'] = linhas_saldo['qtdeRequisitada'] - linhas_saldo['qtd']

            # 4. Limpa o endereço e força o status de reserva da nova linha de saldo
            linhas_saldo['endereco'] = "-"
            linhas_saldo['endereco_reservado'] = "Não Reposto"

            # 5. Ajusta a quantidade da linha original para o que tem de fato no estoque
            # Descomente a linha abaixo se o seu processo de separação exigir que a linha
            # original mostre apenas a quantidade parcial que será atendida naquele momento:
            # consulta.loc[mask_saldo, 'qtdeRequisitada'] = consulta.loc[mask_saldo, 'qtd']

            # 6. Concatena as novas linhas geradas de volta ao DataFrame principal
            consulta = pd.concat([consulta, linhas_saldo], ignore_index=True)

            # 7. Ordena o DataFrame para manter o código do item e a requisição agrupados
            consulta = consulta.sort_values(by=['req', 'codItem']).reset_index(drop=True)

        return consulta