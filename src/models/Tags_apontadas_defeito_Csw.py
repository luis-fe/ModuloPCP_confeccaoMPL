import pandas as pd
from src.connection import ConexaoERP, ConexaoPostgre
from src.models import ServicoAutomacao

class Tags_apontada_defeitos():
    '''Classe que gerencia a compilacao das tags apontadas pelo motivo de segunda no csw '''


    def __init__(self, codEmpresa = '1', intervalo_automacao = 100, n_dias_historico = 40):

        self.codEmpresa = codEmpresa
        self.intervalo_automacao = intervalo_automacao # Atrubuto para Controlar o intervalo de automacao em Segundos
        self.n_dias_historico = n_dias_historico


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

        return motivos

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
            '''Metodo publico que insere informacoes levantadas no postgre'''
            servicoAutomacao = ServicoAutomacao.ServicoAutomacao('',
                                                                 f'Busca de tags dos ultimos {str(self.n_dias_historico)} dias')
            self.ultima_atualizacao = servicoAutomacao.obtentendo_intervalo_atualizacao_rotina()
            print(f'ultima atualizacao {self.ultima_atualizacao}')

            if self.ultima_atualizacao > self.intervalo_automacao:

                dados_tags_defeito =self.tags_defeitos_n_dias_anteriores()
                historico = self.__renovando_historico_Tags()
                historico['excluir'] = 'ok'

                dados_tags_defeito = pd.merge(dados_tags_defeito,historico,on='numeroOP',how='left')
                dados_tags_defeito.fillna('-',inplace=True)
                dados_tags_defeito = dados_tags_defeito[dados_tags_defeito['excluir'] =='-']
                dados_tags_defeito.drop('excluir', axis=1, inplace=True)

                motivos = self.motivos_csw()
                dados_tags_defeito = pd.merge(dados_tags_defeito,motivos,on='motivo2Qualidade',how='left')

                dataHora = servicoAutomacao.obterHoraAtual()
                servicoAutomacao.inserindo_automacao(dataHora)
                ConexaoPostgre.Funcao_InserirPCPMatriz(dados_tags_defeito, dados_tags_defeito['numeroOP'].size, 'tags_defeitos_csw', 'replace')

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





