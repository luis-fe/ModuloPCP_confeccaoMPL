from datetime import datetime

import pandas as pd
import pytz

from src.connection import ConexaoERP, ConexaoPostgre
from src.models import ServicoAutomacao

class Tags_apontada_defeitos():
    '''Classe que gerencia a compilacao das tags apontadas pelo motivo de segunda no csw '''


    def __init__(self, codEmpresa = '1', intervalo_automacao = 100, n_dias_historico = 40):

        self.codEmpresa = codEmpresa
        self.intervalo_automacao = intervalo_automacao # Atrubuto para Controlar o intervalo de automacao em Segundos
        self.n_dias_historico = n_dias_historico

        ''''Metodo publico que insere informacoes levantadas no postgre'''
        self.servicoAutomacao = ServicoAutomacao.ServicoAutomacao('1',
                                                         f'Busca de tags dos ultimos {str(self.n_dias_historico)} dias')

    def motivos_csw(self):
        '''Metodo que busca os motivos cadastros no csw'''

        motivos = f"""
            SELECT 
                codMotivo as motivo2Qualidade , 
                nome, 
                codOrigem,
                (SELECT o.nome from tcp.OrgSegQualidade o WHERE o.empresa = {self.codEmpresa}  and o.codorigem = m.codorigem) as nomeOrigem
            FROM 
                tcp.Mot2Qualidade m 
            WHERE 
                m.Empresa = {self.codEmpresa} 
                """
        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(motivos)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                motivos = pd.DataFrame(rows, columns=colunas)
                del rows, colunas

            return motivos

    def controle_recebimento_csw(self):
        '''Metodo publico que busca a data e hora do controle de recebimento de uma OP Pai'''

        select = f"""
        SELECT
            SUBSTRING(numeroOp,1,10) as numeroOp,
            min(c.dataFimProcesso) as data_receb,
            min(horaFimProcesso) as horaFimProcesso
        FROM
            tco.ControleReceb c
        WHERE 
            c.codEmpresa = {self.codEmpresa} 
        group by SUBSTRING(numeroOp,1,10)
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(select)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows, colunas

            return consulta

    def tags_defeitos_n_dias_anteriores(self):
        '''metodo publico que busca no erp csw as tags dos ultimos n dias com defeito apontado'''

        sql =f"""
        SELECT 
            codempresa,
            count (codBarrasTag) as qtd , 
            codReduzido , 
            numeroOP , 
            CONVERT(VARCHAR(6), numeroOP) as OPpai,
            motivo2Qualidade  
        FROM 
            tcr.TagBarrasProduto t
        WHERE 
            t.codEmpresa = {self.codEmpresa} 
            and t.numeroOP in
                (
                SELECT 
                    op.numeroop 
                from 
                    tco.MovimentacaoOPFase op 
                WHERE 
                    op.codempresa = {self.codEmpresa} and op.codfase in (429, 441, 449)
                    and op.datamov >= (NOW()-'{str(self.n_dias_historico)} days') 
                ) 
        and motivo2Qualidade > 0 and situacao <> 1
        group by 
            codempresa,
            codReduzido , 
            numeroOP , 
            CONVERT(VARCHAR(6), numeroOP),
            motivo2Qualidade  
            """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sql)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                dados_tags_defeito = pd.DataFrame(rows, columns=colunas)
                del rows, colunas

            return dados_tags_defeito
    def inserindo_informacoes_tag_postgre(self):

            self.ultima_atualizacao = self.servicoAutomacao.obtentendo_intervalo_atualizacao_servico()
            print(f'ultima atualizacao {self.ultima_atualizacao}')

            if self.ultima_atualizacao > self.intervalo_automacao:

                dataHora = self.servicoAutomacao.obterHoraAtual()
                self.servicoAutomacao.inserindo_automacao(dataHora)

                controleRecebimento = self.controle_recebimento_csw()
                controleRecebimento['numeroOp'] = controleRecebimento['numeroOp'].astype(str)
                dados_tags_defeito =self.tags_defeitos_n_dias_anteriores()
                dados_tags_defeito = pd.merge(dados_tags_defeito, controleRecebimento, on='numeroOp',how='left')

                dataHora = self.servicoAutomacao.obterHoraAtual()
                self.servicoAutomacao.update_controle_automacao('etapa 1 - Busca sql',dataHora)
                historico = self.__renovando_historico_Tags()
                historico['excluir'] = 'ok'

                dados_tags_defeito = pd.merge(dados_tags_defeito,historico,on='numeroOP',how='left')
                dados_tags_defeito.fillna('-',inplace=True)
                dados_tags_defeito = dados_tags_defeito[dados_tags_defeito['excluir'] =='-']
                dados_tags_defeito.drop('excluir', axis=1, inplace=True)

                motivos = self.motivos_csw()

                dados_tags_defeito = pd.merge(dados_tags_defeito,motivos,on='motivo2Qualidade',how='left')

                if dados_tags_defeito['numeroOP'].size > 0:
                    dados_tags_defeito['data_hora'] = self.obterHoraAtual()
                    dataHora = self.servicoAutomacao.obterHoraAtual()
                    self.servicoAutomacao.update_controle_automacao(f'Finalizado tags inseridas {dados_tags_defeito["numeroOP"].size }', dataHora)
                    ConexaoPostgre.Funcao_InserirPCPMatriz(dados_tags_defeito, dados_tags_defeito['numeroOP'].size, 'tags_defeitos_csw', 'append')
                else:
                    self.servicoAutomacao.update_controle_automacao('Finalizado sem Tags', dataHora)


    def __renovando_historico_Tags(self):
        '''Metodo privado que exclui as tags para realizar a RENOVACAO'''

        sql = """
        select
            distinct "numeroOP"
        from
            "PCP".pcp.tags_defeitos_csw o 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn)


        return consulta

    def obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora





