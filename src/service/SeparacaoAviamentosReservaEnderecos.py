from src.models import Endereco_aviamento
import numpy as np
import pandas as pd


class Reserva_Enderecos():

    def __init__(self, codEmpresa='1'):
        self.codEmpresa = codEmpresa

    def carregar_tabela_reserva_enderecos(self):
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

        # 1 - Get das requisicoes em aberto
        consulta1 = endereco_aviamento.get_requisicao_itens()

        # 2 - Get do mapa COMPLETO de enderecos (sem filtrar ainda)
        mapa_enderecos_completo = endereco_aviamento.get_itens_repostos_para_reserva()
        mapa_enderecos_completo['ocorrencia_acumulada'] = mapa_enderecos_completo.groupby(['codItem']).cumcount() + 1

        # Mapeamos as colunas que vêm do banco para poder limpá-las antes do Ciclo 2
        colunas_do_mapa = ['endereco', 'qtd', 'tipoControle', 'ocorrencia_acumulada']

        # ==========================================
        # CICLO 1: Busca no primeiro endereço
        # ==========================================
        mapa_ciclo_1 = mapa_enderecos_completo[mapa_enderecos_completo['ocorrencia_acumulada'] == 1]

        # Primeiro merge
        consulta = pd.merge(consulta1, mapa_ciclo_1, on='codItem', how='left')
        consulta.fillna('', inplace=True)

        consulta['qtd'] = pd.to_numeric(consulta['qtd'], errors='coerce').fillna(0)
        consulta['qtdeRequisitada'] = pd.to_numeric(consulta['qtdeRequisitada'], errors='coerce').fillna(0)

        # Preserva a quantidade original pedida
        consulta['qtdeRequisitada_original'] = consulta['qtdeRequisitada']

        # Define status de reserva do Ciclo 1
        consulta['endereco_reservado'] = np.where(
            consulta['qtdeRequisitada'] >= consulta['qtd'],
            consulta['endereco'],
            "Não Reposto"
        )

        # Prepara uma lista para ir guardando os DataFrames que serão unidos no final
        dfs_para_concatenar = []

        # Verifica onde faltou estoque no Ciclo 1
        mask_saldo_1 = consulta['qtdeRequisitada'] > consulta['qtd']

        if mask_saldo_1.any():
            # ==========================================
            # PREPARAÇÃO PARA O CICLO 2
            # ==========================================
            # 1. Isola as linhas que precisam buscar mais estoque
            saldo_ciclo_1 = consulta[mask_saldo_1].copy()

            # 2. Atualiza a quantidade que ainda precisa ser buscada (Saldo Novo)
            saldo_ciclo_1['qtdeRequisitada'] = saldo_ciclo_1['qtdeRequisitada'] - saldo_ciclo_1['qtd']

            # 3. Trava a quantidade da linha do Ciclo 1 para o que de fato tinha lá
            consulta.loc[mask_saldo_1, 'qtdeRequisitada'] = consulta.loc[mask_saldo_1, 'qtd']

            # Guarda as linhas prontas do Ciclo 1
            dfs_para_concatenar.append(consulta)

            # 4. LIMPEZA DE COLUNAS: Tira os dados do endereço 1 para receber os dados do endereço 2
            colunas_para_limpar = colunas_do_mapa + ['endereco_reservado', 'index', 'level_0']
            saldo_ciclo_1 = saldo_ciclo_1.drop(
                columns=[col for col in colunas_para_limpar if col in saldo_ciclo_1.columns])

            # ==========================================
            # CICLO 2: Busca no segundo endereço
            # ==========================================
            mapa_ciclo_2 = mapa_enderecos_completo[mapa_enderecos_completo['ocorrencia_acumulada'] == 2]

            # Novo merge: cruza o que faltou com o endereço 2
            consulta_ciclo_2 = pd.merge(saldo_ciclo_1, mapa_ciclo_2, on='codItem', how='left')

            consulta_ciclo_2['qtd'] = pd.to_numeric(consulta_ciclo_2['qtd'], errors='coerce').fillna(0)

            # Define status de reserva do Ciclo 2
            consulta_ciclo_2['endereco_reservado'] = np.where(
                consulta_ciclo_2['qtdeRequisitada'] >= consulta_ciclo_2['qtd'],
                consulta_ciclo_2['endereco'],
                "Não Reposto"
            )

            # ==========================================
            # SALDO FINAL (Caso o Ciclo 2 também não dê conta)
            # ==========================================
            mask_saldo_2 = consulta_ciclo_2['qtdeRequisitada'] > consulta_ciclo_2['qtd']

            if mask_saldo_2.any():
                # Isola o que não foi atendido nem no 1º nem no 2º endereço
                saldo_final = consulta_ciclo_2[mask_saldo_2].copy()
                saldo_final['qtdeRequisitada'] = saldo_final['qtdeRequisitada'] - saldo_final['qtd']
                saldo_final['endereco'] = "-"
                saldo_final['endereco_reservado'] = "Não Reposto"

                # Trava a quantidade do Ciclo 2 para o que de fato tinha lá
                consulta_ciclo_2.loc[mask_saldo_2, 'qtdeRequisitada'] = consulta_ciclo_2.loc[mask_saldo_2, 'qtd']

                # Guarda as linhas do Ciclo 2 e as linhas do Saldo Final
                dfs_para_concatenar.extend([consulta_ciclo_2, saldo_final])
            else:
                # Se o Ciclo 2 atendeu tudo o que faltava, guarda só ele
                dfs_para_concatenar.append(consulta_ciclo_2)

        else:
            # Se o Ciclo 1 já atendeu tudo de primeira, só precisamos dele
            dfs_para_concatenar.append(consulta)

        # ==========================================
        # MONTAGEM FINAL
        # ==========================================
        # Junta todos os cenários (Ciclo 1 + Ciclo 2 + Saldos Finais)
        df_final = pd.concat(dfs_para_concatenar, ignore_index=True)

        # Ordena para o operador de separação ver o item agrupado na mesma requisição
        df_final = df_final.sort_values(by=['req', 'codItem', 'ocorrencia_acumulada']).reset_index(drop=True)

        return df_final