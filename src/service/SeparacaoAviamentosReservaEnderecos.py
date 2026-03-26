from src.models import Endereco_aviamento
import numpy as np
import pandas as pd


class Reserva_Enderecos():

    def __init__(self, codEmpresa='1'):
        self.codEmpresa = codEmpresa

    def carregar_tabela_reserva_enderecos(self):
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

        # 1 - Get das requisições em aberto e ajuste de tipos
        consulta1 = endereco_aviamento.get_requisicao_itens()
        consulta1['qtdeRequisitada'] = pd.to_numeric(consulta1['qtdeRequisitada'], errors='coerce').fillna(0)
        consulta1['qtdeRequisitada_original'] = consulta1['qtdeRequisitada']  # Preserva o original

        # 2 - Get do mapa COMPLETO de endereços (sem filtrar ocorrência ainda)
        mapa_enderecos_completo = endereco_aviamento.get_itens_repostos_para_reserva()
        mapa_enderecos_completo['ocorrencia_acumulada'] = mapa_enderecos_completo.groupby(['codItem']).cumcount() + 1

        # Variáveis de controle para o Loop
        colunas_do_mapa = ['endereco', 'qtd', 'tipoControle', 'ocorrencia_acumulada']
        dfs_para_concatenar = []

        # df_atual vai guardar sempre o que "falta" ser atendido. Começa com todas as requisições.
        df_atual = consulta1.copy()

        # ==========================================
        # LOOP DE BUSCA: Itera até 7 vezes (Endereços 1 a 7)
        # ==========================================
        for ciclo in range(1, 8):
            # Se não houver mais requisições pendentes, encerra o loop mais cedo e economiza processamento
            if df_atual.empty:
                break

            # Filtra o mapa de endereços para o ciclo atual (1ª, 2ª, 3ª ocorrência, etc.)
            mapa_ciclo = mapa_enderecos_completo[mapa_enderecos_completo['ocorrencia_acumulada'] == ciclo]

            # Cruzamento (Merge) do saldo pendente com o endereço do ciclo atual
            df_merge = pd.merge(df_atual, mapa_ciclo, on='codItem', how='left')
            df_merge['qtd'] = pd.to_numeric(df_merge['qtd'], errors='coerce').fillna(0)
            df_merge['endereco'] = df_merge['endereco'].fillna("-")

            # O endereço fica reservado se tivermos algum estoque nessa iteração
            df_merge['endereco_reservado'] = np.where(df_merge['qtd'] > 0, df_merge['endereco'], "Não Reposto")

            # Identifica onde a requisição é maior que o estoque desta gaveta específica
            mask_saldo = df_merge['qtdeRequisitada'] > df_merge['qtd']

            # --- CENÁRIO A: Linhas 100% atendidas neste ciclo ---
            linhas_atendidas_total = df_merge[~mask_saldo].copy()
            if not linhas_atendidas_total.empty:
                dfs_para_concatenar.append(linhas_atendidas_total)

            # --- CENÁRIO B: Linhas que faltaram estoque (Geram saldo para o próximo ciclo) ---
            if mask_saldo.any():
                linhas_com_falta = df_merge[mask_saldo].copy()

                # 1. Salva a parte que conseguimos atender NESTE ciclo (se houver algum estoque > 0)
                linhas_atendidas_parcial = linhas_com_falta[linhas_com_falta['qtd'] > 0].copy()
                if not linhas_atendidas_parcial.empty:
                    linhas_atendidas_parcial['qtdeRequisitada'] = linhas_atendidas_parcial['qtd']
                    dfs_para_concatenar.append(linhas_atendidas_parcial)

                # 2. Prepara a linha de SALDO que vai seguir para a próxima iteração do loop
                saldo_para_proximo = linhas_com_falta.copy()
                saldo_para_proximo['qtdeRequisitada'] = saldo_para_proximo['qtdeRequisitada'] - saldo_para_proximo[
                    'qtd']

                # Limpa as colunas de endereço que vieram do merge para não dar conflito na próxima volta
                colunas_para_limpar = [col for col in colunas_do_mapa if col in saldo_para_proximo.columns] + [
                    'endereco_reservado']
                df_atual = saldo_para_proximo.drop(columns=colunas_para_limpar)
            else:
                # Se tudo foi atendido, esvaziamos o df_atual para o loop quebrar na próxima volta
                df_atual = pd.DataFrame(columns=df_atual.columns)

        # ==========================================
        # SALDO FINAL DEFINITIVO (Quebras reais)
        # ==========================================
        # Se depois de passar pelas 7 gavetas ainda sobrou algo no df_atual, registramos a quebra definitiva
        if not df_atual.empty:
            df_atual['endereco'] = "-"
            df_atual['qtd'] = 0
            df_atual['ocorrencia_acumulada'] = "-"
            df_atual['endereco_reservado'] = "Não Reposto"
            dfs_para_concatenar.append(df_atual)

        # ==========================================
        # MONTAGEM FINAL
        # ==========================================
        df_final = pd.concat(dfs_para_concatenar, ignore_index=True)

        # Ordena para a tela de separação agrupar pelo pedido e priorizar a ordem dos endereços
        df_final = df_final.sort_values(by=['req', 'codItem', 'ocorrencia_acumulada']).reset_index(drop=True)

        return df_final