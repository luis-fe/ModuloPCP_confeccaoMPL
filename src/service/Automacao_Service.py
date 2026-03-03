import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pytz

from src.models import OrdemProd_Csw, Endereco_aviamento
from src.connection import ConexaoPostgre
from src.models.ServicoAutomacao import ServicoAutomacao

# Configuração do Logger (Você pode salvar isso em um arquivo .log depois)
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

    def __init__(self, codEmpresa='1', intervalo_automacao : int = 600):
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

            # 4. Cruzamento de dados (Merge)
            # Trazemos apenas as colunas necessárias das outras tabelas para manter o df leve
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
            clausula_in = "IN (" + ", ".join(f"'{val}'" for val in ops_unicas) + ")"


            logger.info(f"Buscando requisições para {len(ops_unicas)} OPs únicas.")
            requisicoes = self.ordemProd_csw.explodir_requisicao_opS(clausula_in)



            # 7. Merge final com as requisições e preparo para o Banco de Dados
            df_entrega = pd.merge(df_filtrado, requisicoes, on='numeroOP', how='left')
            df_entrega.fillna('-', inplace=True)  # Preenche vazios apenas no final, antes do banco



            # verificar daos ja imputados




            # 8. Carga de dados no PostgreSQL
            qtd_linhas = df_entrega['numeroOP'].size
            logger.info(f"Iniciando inserção de {qtd_linhas} registros no PostgreSQL.")


            df_entrega['dataHora_informacao'] = self.__obter_data_hora()

            ConexaoPostgre.Funcao_InserirPCPMatriz(
                df_entrega,
                df_entrega['numeroOP'].size,
                'AviamentosDisponiveis',
                'replace'
            )

            self.servicoAutomacao.update_controle_automacao('Finalizado Disponibilidade Aviamentos', self.__obter_data_hora())


            logger.info("Rotina finalizada com sucesso!")





    def __obter_data_hora(self):
        """Metodo privado para obter a dataHora do Sistema Operacional em fuso-br """
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora
