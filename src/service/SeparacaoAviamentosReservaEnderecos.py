from src.models import Endereco_aviamento
import numpy as np
import pandas as pd
from datetime import datetime
import pytz
from src.connection import ConexaoPostgre

class Reserva_Enderecos():

    def __init__(self, codEmpresa='1'):
        self.codEmpresa = codEmpresa

    def carregar_tabela_reserva_enderecos(self):
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

        # 1 - Get das requisições em aberto e ajuste de tipos
        consulta1 = endereco_aviamento.get_requisicao_itens()
        consulta1['qtdeRequisitada'] = pd.to_numeric(consulta1['qtdeRequisitada'], errors='coerce').fillna(0)
        consulta1['qtdeRequisitada_original'] = consulta1['qtdeRequisitada']

        # 2 - Get do mapa de KITS (sem filtrar ocorrência ainda)
        mapa_enderecos_completo = endereco_aviamento.get_itens_repostos_para_reserva()
        mapa_enderecos_completo['ocorrencia_acumulada'] = mapa_enderecos_completo.groupby(['codItem']).cumcount() + 1

        # Variáveis de controle
        colunas_do_mapa = ['endereco', 'qtd', 'tipoControle', 'ocorrencia_acumulada']
        dfs_para_concatenar = []
        df_atual = consulta1.copy()

        # ==========================================
        # FASE 1: BUSCA EM KITS (Loop de 1 a 7)
        # ==========================================
        for ciclo in range(1, 8):
            if df_atual.empty:
                break

            mapa_ciclo = mapa_enderecos_completo[mapa_enderecos_completo['ocorrencia_acumulada'] == ciclo]

            df_merge = pd.merge(df_atual, mapa_ciclo, on='codItem', how='left')
            df_merge['qtd'] = pd.to_numeric(df_merge['qtd'], errors='coerce').fillna(0)
            df_merge['endereco'] = df_merge['endereco'].fillna("Não Reposto")

            # REGRA DO KIT FECHADO
            mask_reserva = (df_merge['qtdeRequisitada'] >= df_merge['qtd']) & (df_merge['qtd'] > 0)

            sucesso = df_merge[mask_reserva].copy()
            if not sucesso.empty:
                linha_separacao = sucesso.copy()
                linha_separacao['qtdeRequisitada'] = linha_separacao['qtd']
                linha_separacao['endereco_reservado'] = linha_separacao['endereco']
                dfs_para_concatenar.append(linha_separacao)

                saldo_sucesso = sucesso.copy()
                saldo_sucesso['qtdeRequisitada'] = saldo_sucesso['qtdeRequisitada'] - saldo_sucesso['qtd']
                saldo_sucesso = saldo_sucesso[saldo_sucesso['qtdeRequisitada'] > 0]
            else:
                saldo_sucesso = pd.DataFrame(columns=df_merge.columns)

            falha = df_merge[~mask_reserva].copy()
            saldo_falha = falha.copy()

            df_proximo_ciclo = pd.concat([saldo_sucesso, saldo_falha], ignore_index=True)
            colunas_para_limpar = [col for col in colunas_do_mapa if col in df_proximo_ciclo.columns] + [
                'endereco_reservado']
            df_atual = df_proximo_ciclo.drop(columns=colunas_para_limpar, errors='ignore')

        # ==========================================
        # FASE 2: BUSCA UNITÁRIA (O que sobrou dos kits)
        # ==========================================
        if not df_atual.empty:
            # Puxa o mapa unitário conforme a sua nova função
            mapa_unitario = endereco_aviamento.get_itens_repostos_para_reserva_unitario()

            # --- CORREÇÃO APLICADA AQUI ---
            # Cria a coluna de ocorrência acumulada para o endereço unitário antes de usá-la
            mapa_unitario['ocorrencia_acumulada'] = mapa_unitario.groupby(['codItem']).cumcount() + 1

            # Garante que a comparação seja feita como string, caso venha assim do seu banco
            mapa_unitario['ocorrencia_acumulada'] = mapa_unitario['ocorrencia_acumulada'].astype(str)
            mapa_unitario = mapa_unitario[mapa_unitario['ocorrencia_acumulada'] == '1']

            # Merge do saldo restante com o endereço unitário
            df_merge_unit = pd.merge(df_atual, mapa_unitario, on='codItem', how='left')
            df_merge_unit['qtd'] = pd.to_numeric(df_merge_unit['qtd'], errors='coerce').fillna(0)
            df_merge_unit['endereco'] = df_merge_unit['endereco'].fillna("Não Reposto")

            # REGRA UNITÁRIA: Pegamos o que der (o menor valor entre a requisição e o estoque)
            df_merge_unit['qtd_atendida_unitaria'] = np.minimum(df_merge_unit['qtdeRequisitada'], df_merge_unit['qtd'])

            # 1. LINHAS ATENDIDAS (Total ou Parcialmente pelo endereço unitário)
            sucesso_unit = df_merge_unit[df_merge_unit['qtd_atendida_unitaria'] > 0].copy()
            if not sucesso_unit.empty:
                linha_sep_unit = sucesso_unit.copy()
                linha_sep_unit['qtdeRequisitada'] = linha_sep_unit['qtd_atendida_unitaria']
                linha_sep_unit['endereco_reservado'] = linha_sep_unit['endereco']

                linha_sep_unit = linha_sep_unit.drop(columns=['qtd_atendida_unitaria'])
                dfs_para_concatenar.append(linha_sep_unit)

            # 2. SALDO FINAL DEFINITIVO (Quebra real - "Não Reposto")
            df_merge_unit['saldo_final'] = df_merge_unit['qtdeRequisitada'] - df_merge_unit['qtd_atendida_unitaria']
            quebra_final = df_merge_unit[df_merge_unit['saldo_final'] > 0].copy()

            if not quebra_final.empty:
                linha_quebra = quebra_final.copy()
                linha_quebra['qtdeRequisitada'] = linha_quebra['saldo_final']
                linha_quebra['endereco'] = "Não Reposto"
                linha_quebra['qtd'] = 0
                linha_quebra['ocorrencia_acumulada'] = "-"
                linha_quebra['endereco_reservado'] = "Não Reposto"

                linha_quebra = linha_quebra.drop(columns=['qtd_atendida_unitaria', 'saldo_final'])
                dfs_para_concatenar.append(linha_quebra)

        # ==========================================
        # MONTAGEM FINAL
        # ==========================================
        df_final = pd.concat(dfs_para_concatenar, ignore_index=True)

        # Opcional: converte ocorrencia_acumulada para string para não dar erro no sort_values
        # já que os kits são int (1 a 7) e os unitários ou quebras podem ser strings ('1', '-')
        df_final['ocorrencia_acumulada'] = df_final['ocorrencia_acumulada'].astype(str)
        df_final = df_final.sort_values(by=['req', 'codItem', 'ocorrencia_acumulada']).reset_index(drop=True)

        df_final.fillna('-',inplace=True)

        df_final['dataHora_informacao'] = self.__obter_data_hora()

        qtd_linhas = len(df_final)

        ConexaoPostgre.Funcao_InserirPCPMatriz(
            df_final,
            qtd_linhas,
            'ReservaAviamentos',
            'replace'
        )

        return df_final


    def __obter_data_hora(self):
        """Metodo privado para obter a dataHora do Sistema Operacional em fuso-br """
        fuso_horario = pytz.timezone('America/Sao_Paulo')
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora

    import pandas as pd