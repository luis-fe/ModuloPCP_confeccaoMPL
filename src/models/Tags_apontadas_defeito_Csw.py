import pandas as pd
from src.connection import ConexaoERP, ConexaoPostgre
from src.models import ServicoAutomacao

class Tags_apontada_defeitos():
    '''Classe que gerencia a compilacao das tags apontadas pelo motivo de segunda no csw '''


    def __init__(self, codEmpresa = '1', intervalo_automacao = 100):

        self.codEmpresa = codEmpresa
        self.intervalo_automacao = intervalo_automacao # Atrubuto para Controlar o intervalo de automacao em Segundos



    def tags_defeitos_n_dias_anteriores(self, n = 40):
        '''metodo publico que busca no erp csw as tags dos ultimos n dias com defeito apontado'''

        sql =f"""
        SELECT 
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
                    and op.datamov >= (NOW()-'{str(n)} days') 
                ) 
        and motivo2Qualidade > 0 and situacao <> 1
        group by 
            codReduzido , 
            numeroOP , 
            CONVERT(VARCHAR(6), numeroOP),
            motivo2Qualidade  
            """


        servicoAutomacao = ServicoAutomacao.ServicoAutomacao('',f'Busca de tags dos ultimos {str(n)} dias')
        self.ultima_atualizacao = servicoAutomacao.obtentendo_intervalo_atualizacao_rotina()
        print(f'ultima atualizacao {self.ultima_atualizacao}')

        if self.ultima_atualizacao > self.intervalo_automacao:
            with ConexaoERP.ConexaoInternoMPL() as conn:
                with conn.cursor() as cursor_csw:
                    # Executa a primeira consulta e armazena os resultados
                    cursor_csw.execute(sql)
                    colunas = [desc[0] for desc in cursor_csw.description]
                    rows = cursor_csw.fetchall()
                    dados_tags_defeito = pd.DataFrame(rows, columns=colunas)
                    del rows, colunas



            dataHora = servicoAutomacao.obterHoraAtual()
            servicoAutomacao.inserindo_automacao(dataHora)
            ConexaoPostgre.Funcao_InserirPCPMatriz(dados_tags_defeito, dados_tags_defeito['numeroOP'].size, 'tags_defeitos_csw', 'replace')

    def __renovando_historico_Tags(self):
        '''Metodo privado que exclui as tags para realizar a RENOVACAO'''





