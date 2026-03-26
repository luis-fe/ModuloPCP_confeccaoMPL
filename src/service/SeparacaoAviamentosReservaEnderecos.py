from src.models import Endereco_aviamento
import numpy as np
import pandas as pd

class Reserva_Enderecos():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa

    import pandas as pd
    import numpy as np

    # ... (seu código anterior do método) ...

    def carregar_tabela_reserva_enderecos(self):
        # ... [código anterior omitido para focar na solução] ...

        consulta['qtd'].fillna(0, inplace=True)

        # --- NOVA LÓGICA COM NUMPY ---
        consulta['endereco_reservado'] = np.where(
            consulta['qtdeRequisitada'] >= consulta['qtd'],  # Condição
            consulta['endereco'],  # Valor se Verdadeiro
            "Não Reposto"  # Valor se Falso
        )

        # --- LÓGICA DE CRIAÇÃO DO SALDO NOVO ---
        # 1. Cria uma máscara booleana para encontrar as linhas onde a requisição é maior que o estoque
        # ... [código anterior do seu método] ...

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

            # 5. Opcional: Ajusta a quantidade da linha original para o que realmente tem no estoque
            # consulta.loc[mask_saldo, 'qtdeRequisitada'] = consulta.loc[mask_saldo, 'qtd']

            # 6. Concatena as novas linhas geradas de volta ao DataFrame principal
            consulta = pd.concat([consulta, linhas_saldo], ignore_index=True)

        return consulta