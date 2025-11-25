import gc
from datetime import datetime

import pandas as pd
import pytz

from src.connection import ConexaoERP, ConexaoPostgre
from src.models import ServicoAutomacao, Produtos_CSW

class Tags_apontada_defeitos():
    '''Classe que gerencia a compilacao das tags apontadas pelo motivo de segunda no csw '''


    def __init__(self, codEmpresa = '1', intervalo_automacao = 100, n_dias_historico = 40, nomeAtualizacao = 'Busca de tags dos ultimos '):

        self.codEmpresa = codEmpresa
        self.intervalo_automacao = intervalo_automacao # Atrubuto para Controlar o intervalo de automacao em Segundos
        self.n_dias_historico = n_dias_historico
        nomeAtualizacao = f'{nomeAtualizacao}{str(self.n_dias_historico)} dias'
        ''''Metodo publico que insere informacoes levantadas no postgre'''
        self.servicoAutomacao = ServicoAutomacao.ServicoAutomacao('1',
                                                                  nomeAtualizacao)

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
            SUBSTRING(numeroOp,1,10) as numeroOP,
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
            codEngenharia,
            (select e.descricao from tcp.Engenharia e 
            WHERE e.codempresa = 1 
            and e.codengenharia = t.codEngenharia) as descProd,
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
        and motivo2Qualidade > 0 and situacao not in (1 , 11, 0, 9)
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
                controleRecebimento['numeroOP'] = controleRecebimento['numeroOP'].astype(str)
                dados_tags_defeito =self.tags_defeitos_n_dias_anteriores()
                dados_tags_defeito = pd.merge(dados_tags_defeito, controleRecebimento, on='numeroOP',how='left')

                conultaOpFinalizada = self.consulta_ops_finalizada_csw()
                dados_tags_defeito = pd.merge(dados_tags_defeito, conultaOpFinalizada, on='numeroOP',how='left')

                dadosFornecedor = Produtos_CSW.Produtos_CSW(self.codEmpresa).materiais_requisicao_OP_csw('160')
                dadosFornecedor = dadosFornecedor.drop_duplicates()

                dados_tags_defeito = pd.merge(dados_tags_defeito, dadosFornecedor, on='OPpai',how='left')


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
                    self.servicoAutomacao.exluir_historico_antes_quarentena()
                    ConexaoPostgre.Funcao_InserirPCPMatriz(dados_tags_defeito, dados_tags_defeito['numeroOP'].size, 'tags_defeitos_csw', 'append')
                else:
                    self.servicoAutomacao.update_controle_automacao('Finalizado sem Tags', dataHora)
                    self.servicoAutomacao.exluir_historico_antes_quarentena()


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


    def consulta_ops_finalizada_csw(self):
        '''Metdo que consulta as OPs finalizadas no csw '''

        consulta = f"""
        select
            numeroOP,
            dataFim as data_fim_op
        FROM
            tco.OrdemProd o
        WHERE
            o.codEmpresa = {self.codEmpresa}
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(consulta)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
                del rows, colunas

            return consulta

    def get_tags_pilotos_csw(self):
        '''Metodo que busca no csw as tags do estoque de pilotos'''



        consulta = f"""
        SELECT
            t.codBarrasTag,
            t.codEngenharia,
            (select s.corbase from tcp.SortimentosProduto s where s.codempresa = 1 
            and s.codproduto = t.codEngenharia and t.codSortimento = s.codsortimento)
            as cor,
            (select s.descricao from tcp.Tamanhos s where s.codempresa = 1 
            and s.sequencia = t.seqTamanho)
            as tamanho,
            (select s.descricao from tcp.Engenharia s where s.codempresa = 1 
            and s.codengenharia = t.codEngenharia)
            as descricao
        FROM
            tcr.TagBarrasProduto t
        WHERE
            t.codEmpresa = {self.codEmpresa}
            and t.situacao = 3
            and t.codNaturezaAtual = 24
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consulta)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta_csw = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()
        dataHora = self.servicoAutomacao.obterHoraAtual()

        tagsAtuais_postgres = self.consultar_tags_pilotos_atuais()

        consulta = pd.merge(consulta_csw, tagsAtuais_postgres , on='codBarrasTag', how = 'left')

        consulta2 = pd.merge(
            tagsAtuais_postgres,
            consulta_csw,
            on='codBarrasTag',
            how='left',
            indicator=True  # Adiciona uma coluna especial '_merge'
        )

        # Filtra apenas as linhas onde a coluna '_merge' é 'left_only'
        resultado_final = consulta2[consulta2['_merge'] == 'left_only']
        resultado_final = resultado_final.drop(columns='_merge')

        if resultado_final['codBarrasTag'].size > 0:
            # 1. Obter a lista de valores da coluna e formatar
            # Transforma a Series em uma lista Python
            lista_codigos = resultado_final['codBarrasTag'].to_list()

            # Formata a lista para o padrão SQL: ('valor1', 'valor2', 'valor3')
            # Use aspas simples (') em torno de cada valor, essencial para strings em SQL.
            valores_sql = str(tuple(lista_codigos)).replace(",)", ")")  # Remove vírgula extra no caso de 1 item

            # 2. Concatena a instrução DELETE com a lista de valores
            delete_query = f"""
            delete from "PCP".pcp."tags_piloto_csw" 
             where "codBarrasTag" in {valores_sql}
            """

            with ConexaoPostgre.conexaoInsercao() as conn2:
                with conn2.cursor() as curr:

                    curr.execute(delete_query,)
                    conn2.commit()

        consulta = consulta[consulta['status']!='OK']
        consulta = consulta.drop(columns=['status'])
        inventario = self.__ultimo_inventario_tag()
        consulta = pd.merge(consulta, inventario, on='codBarrasTag', how='left')



        if consulta['codBarrasTag'].size > 0:
            consulta['dataHora'] = self.obterHoraAtual()
            self.servicoAutomacao.update_controle_automacao(
                f'Finalizado tags inseridas {consulta["codBarrasTag"].size}', dataHora)
            self.servicoAutomacao.exluir_historico_antes_quarentena()
            ConexaoPostgre.Funcao_InserirPCPMatriz(consulta, consulta['codBarrasTag'].size,
                                                   'tags_piloto_csw', 'append')
        else:
            self.servicoAutomacao.update_controle_automacao('Finalizado sem Tags', dataHora)
            self.servicoAutomacao.exluir_historico_antes_quarentena()

    def consultar_tags_pilotos_atuais(self):

        consulta = f"""
                select 
                    "codBarrasTag", 
                    'OK' as "status" 
                from 
                    "PCP".pcp."tags_piloto_csw" 
        """


        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)


        return consulta


    def __ultimo_inventario_tag(self):


        sql = """
        SELECT 
            convert(varchar(40),t.codBarrasTag) as codBarrasTag, 
            ip.dataEncContagem as ultimoInv
        FROM tci.InventarioProdutosTagLidas t
        INNER JOIN tci.InventarioProdutos ip 
            ON ip.Empresa = 1 
            AND t.inventario = ip.inventario 
        WHERE t.Empresa = 1 
          AND ip.codnatureza = 24 
          AND ip.situacao = 4
          AND ip.dataEncContagem = (
                SELECT MAX(ip2.dataEncContagem)
                FROM tci.InventarioProdutos ip2
                WHERE ip2.Empresa = ip.Empresa
                  AND ip2.codnatureza = ip.codnatureza
            )
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta






