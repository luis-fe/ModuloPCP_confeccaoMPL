import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

from src.models import OrdemProd_Csw, Endereco_aviamento, Produtos_CSW, MateriaPrima
from src.connection import ConexaoPostgre
from src.models.ServicoAutomacao import ServicoAutomacao

# Configuração do Logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class Automacao:
    '''
    Classe responsável pelo Serviço de controle de automação de dados
    para retroalimentar o sistema de Gestão Industrial e PCP.

    CATÁLOGO DE SERVIÇOS:
    1- Produção Realizada das Fases Produtivas;
    2- Tags Apontadas com defeito;
    3- Informações de Faturamento;
    4- Disposição de Aviamentos no status A Aviamentar.

    Objetivo do Serviço:
    EXTRAIR-TRANSFORMAR-CARREGAR dados para operação do Ecossistema de Gestão Industrial e PCP.
    '''

    # Constantes da classe (facilita a manutenção caso os códigos mudem no ERP)
    FASE_SEPARACAO = '409'
    FASE_COSTURA = '428'

    def __init__(self, codEmpresa='1', intervalo_automacao: int = 600):
        self.codEmpresa = codEmpresa
        self.ordemProd_csw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa)
        self.intervalo_automacao = intervalo_automacao

    def buscar_informacao_aviamentos_disponiveis_CSW(self):
        '''
        Método que busca e processa as informações dos aviamentos disponíveis para separação,
        e alimenta o Postgres no disparo da Automação, atendendo ao item:
            4- Disposição de Aviamentos no status A Aviamentar;
        '''

        self.servicoAutomacao = ServicoAutomacao('004','Disposição de Aviamentos no status A Aviamentar')
        self.ultima_atualizacao = self.servicoAutomacao.obtentendo_intervalo_atualizacao_servico()

        if self.ultima_atualizacao > self.intervalo_automacao:

            endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

            logger.info("Iniciando rotina: Disposição de Aviamentos no status A Aviamentar")

            self.servicoAutomacao.inserindo_automacao(self.__obter_data_hora())

            # 1. Buscando as informações das OPs em aberto
            df_aberto = self.ordemProd_csw.ordem_Prod_em_aberto()

            if df_aberto is None or df_aberto.empty:
                logger.warning("Nenhuma OP em aberto encontrada no CSW.")
                return

            # 2. Buscando OPs que passaram pela fase de Separação (409)
            self.ordemProd_csw.codFase = self.FASE_SEPARACAO
            df_separacao = self.ordemProd_csw.ops_emAberto_movimentacao_fase()
            df_separacao['passou_separacao'] = 'sim'

            # 3. Buscando OPs que passaram pela fase de Costura (428)
            self.ordemProd_csw.codFase = self.FASE_COSTURA
            df_costura = self.ordemProd_csw.ops_emAberto_movimentacao_fase()
            df_costura['passou_costura'] = 'sim'

            # 3.1. Excluindo da fila de Disponibilidade as OPs processadas pela fase 428
            ops_processadas = df_costura['numeroOP'].dropna().unique()

            # Formatação segura da cláusula IN
            clausula_in_ops_processadas = "IN ('" + "','".join(f"{val}" for val in ops_processadas) + "')"

            endereco_aviamento.delete_ops_processadas_AviamentosDisponives(clausula_in_ops_processadas)



            # 4. Cruzamento de dados (Merge)
            df_final = pd.merge(df_aberto, df_separacao[['numeroOP', 'passou_separacao']], on='numeroOP', how='left')
            df_final = pd.merge(df_final, df_costura[['numeroOP', 'passou_costura']], on='numeroOP', how='left')

            # 5. Lógica de Filtragem Direta: Passou pela separação, mas ainda NÃO passou pela costura
            mask = (df_final['passou_costura'].isna()) & (df_final['passou_separacao'] == 'sim')
            df_filtrado = df_final[mask].copy()
            df_filtrado['situacaoOP'] = 'Em Operacao Almoxarifado'

            if df_filtrado.empty:
                logger.info("Nenhuma OP atende aos critérios para processamento de aviamentos no momento.")
                return

            # 6. Preparar a consulta de requisições para as OPs filtradas
            ops_unicas = df_filtrado['numeroOP'].dropna().unique()

            # Formatação segura da cláusula IN
            clausula_in = "IN ('" + "','".join(f"{val}" for val in ops_unicas) + "')"

            logger.info(f"Buscando requisições para {len(ops_unicas)} OPs únicas.")
            requisicoes = self.ordemProd_csw.explodir_requisicao_opS(clausula_in)
            requisicoes['numero'] = requisicoes['numero'].astype(str)

            # 7. Merge final com as requisições e preparo para o Banco de Dados
            df_entrega = pd.merge(df_filtrado, requisicoes, on='numeroOP', how='left')
            df_entrega.fillna('-', inplace=True)




            itens_restricao = endereco_aviamento.update_desconsidera_item_aviamento2()


            # --- INÍCIO DA VALIDAÇÃO DE REGISTROS NOVOS ---

            # 7.1 Obter chaves existentes
            consultaChaves = endereco_aviamento.get_chaves_consulta_AviamentosDisponiveis()

            # 7.2 Identificar novos registros via Left Join
            df_entrega = pd.merge(
                df_entrega,
                consultaChaves,
                on=['numeroOP', 'codMaterialEdt'],
                how='left',
                indicator=True
            )

            # 7.3 Filtrar apenas o que não existe no banco (left_only)
            df_novos = df_entrega[df_entrega['_merge'] == 'left_only'].copy()

            # 7.4 Limpeza de colunas auxiliares (AQUI RESOLVEMOS O ERRO)
            # errors='ignore' garante que o código não quebre caso a coluna já não exista
            colunas_indesejadas = ['_merge', 'situacao']
            df_novos.drop(columns=colunas_indesejadas, inplace=True, errors='ignore')

            # 8. Carga de dados
            if not df_novos.empty:
                qtd_linhas = len(df_novos)
                logger.info(f"Iniciando inserção de {qtd_linhas} novos registros no PostgreSQL.")

                df_novos['dataHora_informacao'] = self.__obter_data_hora()

                ConexaoPostgre.Funcao_InserirPCPMatriz(
                    df_novos,
                    qtd_linhas,
                    'AviamentosDisponiveis',
                    'append'
                )

                self.servicoAutomacao.update_controle_automacao('Finalizado Disponibilidade Aviamentos v3', self.__obter_data_hora())
            else:
                self.servicoAutomacao.update_controle_automacao('Finalizado Disponibilidade Aviamentos v3, sem dados novos', self.__obter_data_hora())
                logger.info("Nenhum registro novo para inserir no PostgreSQL.")

            logger.info("Rotina finalizada com sucesso!")

    def __obter_data_hora(self):
        """Metodo privado para obter a dataHora do Sistema Operacional em fuso-br """
        fuso_horario = pytz.timezone('America/Sao_Paulo')
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora

    import pandas as pd

    def recebimento_aviamentos_CSW(self):
        '''Automação que busca os aviamentos disponíveis para repor'''

        self.servicoAutomacao = ServicoAutomacao('007', 'Fila de Aviamentos a repor')
        self.ultima_atualizacao = self.servicoAutomacao.obtentendo_intervalo_atualizacao_servico()

        if self.ultima_atualizacao > self.intervalo_automacao:
            # 1. Busca de dados
            fila = Produtos_CSW.Produtos_CSW(self.codEmpresa).estoqueNat_aviamentos()
            categorias = MateriaPrima.Materia_prima_aviamento(self.codEmpresa).configuracao_de_para_descricao()
            consulta = Endereco_aviamento.Endereco_aviamento().get_consultar_items_repostos()

            # 2. Categorização
            fila['nome_limpo'] = fila['nome'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
                'utf-8').str.upper()
            fila['categoria'] = 'Outros'

            for _, regra in categorias.iterrows():
                gatilho = regra['descricao_contem'].upper()
                categoria_destino = regra['categoria']
                mascara = fila['nome_limpo'].str.contains(gatilho, na=False)
                fila.loc[mascara, 'categoria'] = categoria_destino

            fila = fila.drop(columns=['nome_limpo'])

            # 3. Cruzamento de Dados (Merge) e Cálculo de Saldo
            # O cálculo deve ser feito ANTES da formatação para string
            fila = pd.merge(fila, consulta, on='codEditado_x', how='left')

            fila['saldoEnderecado'] = fila['saldoEnderecado'].fillna(0)

            # Garante que são números antes da subtração
            fila['estoqueAtual'] = pd.to_numeric(fila['estoqueAtual'], errors='coerce').fillna(0)
            fila['estoque_calculado'] = fila['estoqueAtual'] - fila['saldoEnderecado']

            # 4. Funções de Formatação
            def formatar_inteiro_milhar(valor):
                try:
                    return f"{int(float(valor)):,}".replace(',', '.')
                except:
                    return str(valor)

            def formatar_decimal_ptbr(valor):
                try:
                    return f"{float(valor):,.3f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                except:
                    return str(valor)

            # 5. Aplicação da Formatação Visual
            mascara_un = fila['unidadeMedida'].isin(['UM', 'UN'])

            # Aplicamos a formatação no estoque calculado
            fila.loc[mascara_un, 'estoque_final_str'] = fila.loc[mascara_un, 'estoque_calculado'].apply(
                formatar_inteiro_milhar)
            fila.loc[~mascara_un, 'estoque_final_str'] = fila.loc[~mascara_un, 'estoque_calculado'].apply(
                formatar_decimal_ptbr)

            # Padronização de nomes de unidade
            fila['unidadeMedida'] = fila['unidadeMedida'].replace({'UM': 'Unid', 'UN': 'Unid'})

            # Preenchimento de nulos finais para o Banco de Dados
            fila = fila.fillna('-')

            # 6. Inserção no Banco
            # Nota: Usei 'replace' (corrigindo o erro de digitação 'replance')
            ConexaoPostgre.Funcao_InserirPCPMatriz(
                fila,
                len(fila),
                'FilaAviamentos',
                'replace'
            )








