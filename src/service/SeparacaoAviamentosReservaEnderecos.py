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

        # df_atual guarda o que "falta" atender. Começa com todas as requisições.
        df_atual = consulta1.copy()

        # ==========================================
        # LOOP DE BUSCA: Itera até 7 vezes (Endereços 1 a 7)
        # ==========================================
        for ciclo in range(1, 8):
            if df_atual.empty:
                break

            # Filtra o mapa de endereços para o ciclo atual
            mapa_ciclo = mapa_enderecos_completo[mapa_enderecos_completo['ocorrencia_acumulada'] == ciclo]

            # Cruzamento (Merge) do saldo pendente com o endereço do ciclo atual
            df_merge = pd.merge(df_atual, mapa_ciclo, on='codItem', how='left')
            df_merge['qtd'] = pd.to_numeric(df_merge['qtd'], errors='coerce').fillna(0)
            df_merge['endereco'] = df_merge['endereco'].fillna("Não Reposto")

            # --- A REGRA DO KIT FECHADO ---
            # Só podemos reservar se precisarmos de uma quantidade MAIOR OU IGUAL ao kit disponível.
            # Se o kit (qtd) for maior que a requisição, NÃO podemos quebrar o kit (mask_reserva = False)
            mask_reserva = (df_merge['qtdeRequisitada'] >= df_merge['qtd']) & (df_merge['qtd'] > 0)

            # 1. PROCESSA QUEM PODE RESERVAR NESTA GAVETA
            sucesso = df_merge[mask_reserva].copy()
            if not sucesso.empty:
                # Linha que vai para a separação (assume o valor do kit inteiro)
                linha_separacao = sucesso.copy()
                linha_separacao['qtdeRequisitada'] = linha_separacao['qtd']
                linha_separacao['endereco_reservado'] = linha_separacao['endereco']
                dfs_para_concatenar.append(linha_separacao)

                # Calcula o que ainda falta pedir e manda para a próxima iteração
                saldo_sucesso = sucesso.copy()
                saldo_sucesso['qtdeRequisitada'] = saldo_sucesso['qtdeRequisitada'] - saldo_sucesso['qtd']
                saldo_sucesso = saldo_sucesso[
                    saldo_sucesso['qtdeRequisitada'] > 0]  # Só passa pra frente se sobrar algo
            else:
                saldo_sucesso = pd.DataFrame(columns=df_merge.columns)

            # 2. PROCESSA QUEM NÃO PODE RESERVAR (Kit muito grande ou sem estoque)
            falha = df_merge[~mask_reserva].copy()
            # O saldo de quem falhou é a própria requisição intacta, pois não pegamos nada daqui
            saldo_falha = falha.copy()

            # 3. JUNTA OS SALDOS PARA O PRÓXIMO CICLO
            df_proximo_ciclo = pd.concat([saldo_sucesso, saldo_falha], ignore_index=True)

            # Limpa as colunas do endereço atual para não sujar o merge da próxima volta
            colunas_para_limpar = [col for col in colunas_do_mapa if col in df_proximo_ciclo.columns] + [
                'endereco_reservado']
            df_atual = df_proximo_ciclo.drop(columns=colunas_para_limpar, errors='ignore')

        # ==========================================
        # SALDO FINAL DEFINITIVO (O que não coube em nenhum kit)
        # ==========================================
        if not df_atual.empty:
            df_atual['endereco'] = "Não Reposto"
            df_atual['qtd'] = 0
            df_atual['ocorrencia_acumulada'] = "-"
            df_atual['endereco_reservado'] = "Não Reposto"
            dfs_para_concatenar.append(df_atual)

        # ==========================================
        # MONTAGEM FINAL
        # ==========================================
        df_final = pd.concat(dfs_para_concatenar, ignore_index=True)

        # Ordenação final para a tela de separação
        df_final = df_final.sort_values(by=['req', 'codItem', 'ocorrencia_acumulada']).reset_index(drop=True)

        return df_final