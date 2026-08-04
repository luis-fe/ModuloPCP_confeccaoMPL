'''Model da meta mensal do indice de 2a Qualidade - tabela pcp."MetaQualide"'''
import unicodedata

import pandas as pd

from src.connection import ConexaoPostgre


class Meta_Qualidade():
    '''
    Classe responsavel pela meta mensal do indice de 2a Qualidade.

    Tabela: pcp."MetaQualide" ( "mes" varchar, "ano" varchar, "meta" numeric(10,2) )

    UNIDADE DA COLUNA "meta":
        A coluna é numeric(10,2), portanto guarda apenas 2 casas decimais - a fracao
        0.015 (1,50%) seria arredondada para 0.02. Por isso o BANCO GUARDA O
        PERCENTUAL (1.50 = 1,50%) e a conversao para a fracao usada pelo frontend
        fica isolada em fracao_para_percentual / percentual_para_fracao.

    OBS: a tabela nao possui chave/indice unico em (mes, ano), por isso a gravacao
    é feita como UPDATE e, quando nao encontra a linha, INSERT.
    '''

    # Mesmo padrao de "mes" usado em "DashbordTV".metas (ver DashboardTV.py):
    # o prefixo numerico é o que garante o "order by mes" na ordem do calendario.
    MESES = [
        '01-Janeiro', '02-Fevereiro', '03-Março', '04-Abril', '05-Maio', '06-Junho',
        '07-Julho', '08-Agosto', '09-Setembro', '10-Outubro', '11-Novembro', '12-Dezembro'
    ]

    # 1,50% - meta exibida no painel de indice quando o mes nao tem meta cadastrada
    META_PADRAO = 1.50

    def __init__(self, ano='', mes='', meta=''):
        '''
        :param ano: ano da meta (varchar no banco, ex.: '2026')
        :param mes: mes no padrao da coluna ('01-Janeiro'); aceita variacoes
                    resolvidas por resolver_mes()
        :param meta: meta em PERCENTUAL (1.50 = 1,50%), unidade da coluna
        '''
        self.ano = str(ano)
        self.mes = mes
        self.meta = meta

    def consultar_metas_ano(self):
        '''
        Metodo publico que obtem as metas cadastradas de um ano.

        :return:
            DataFrame (pandas) com as colunas mes, ano e meta, ordenado pelo mes
        '''
        consulta = '''
            select
                "mes",
                "ano",
                "meta"
            from
                pcp."MetaQualide"
            where
                "ano" = %s
            order by
                "mes"
        '''

        conn = ConexaoPostgre.conexaoEngine()

        return pd.read_sql(consulta, conn, params=(self.ano,))

    def consultar_meta_mes(self):
        '''
        Metodo publico que obtem a meta de um mes especifico do ano.

        :return:
            DataFrame (pandas) com as colunas mes, ano e meta (vazio se nao cadastrada)
        '''
        consulta = '''
            select
                "mes",
                "ano",
                "meta"
            from
                pcp."MetaQualide"
            where
                "ano" = %s
                and "mes" = %s
        '''

        conn = ConexaoPostgre.conexaoEngine()

        return pd.read_sql(consulta, conn, params=(self.ano, self.mes))

    def inserir_ou_atualizar_meta(self):
        '''
        Metodo publico que grava a meta do mes/ano. Atualiza a linha existente e,
        se ela ainda nao existir, faz o insert.

        :return:
            'atualizado' ou 'inserido'
        '''
        update = '''
            update
                pcp."MetaQualide"
            set
                "meta" = %s
            where
                "ano" = %s
                and "mes" = %s
        '''

        insert = '''
            insert into pcp."MetaQualide"
                ( "mes", "ano", "meta" )
            values
                ( %s, %s, %s )
        '''

        conn = ConexaoPostgre.conexaoInsercao()

        try:
            with conn.cursor() as curr:
                curr.execute(update, (self.meta, self.ano, self.mes))

                if curr.rowcount == 0:
                    curr.execute(insert, (self.mes, self.ano, self.meta))
                    operacao = 'inserido'
                else:
                    operacao = 'atualizado'

            conn.commit()
        finally:
            conn.close()

        return operacao

    @classmethod
    def resolver_mes(cls, mes, indice=None):
        '''
        Metodo publico que devolve o mes no padrao da coluna ('01-Janeiro').

        Aceita '01-Janeiro', 'Janeiro', 'janeiro', 'MARÇO', 'Marco', '3', '03' e,
        quando nada casa, cai na posicao informada em "indice" (0 = Janeiro).

        :return:
            mes no padrao da coluna ou None quando nao é possivel resolver
        '''
        texto = cls.__sem_acento(str(mes).strip().lower())

        for posicao, canonico in enumerate(cls.MESES):
            numero, nome = canonico.split('-')
            nome = cls.__sem_acento(nome.lower())

            if texto in (cls.__sem_acento(canonico.lower()), nome, numero, str(posicao + 1)):
                return canonico

        if indice is not None and 0 <= indice < len(cls.MESES):
            return cls.MESES[indice]

        return None

    @classmethod
    def nome_do_mes(cls, mes):
        '''
        Metodo publico que devolve so o nome do mes ('01-Janeiro' -> 'Janeiro'),
        formato consumido pelo frontend.
        '''
        return str(mes).split('-')[-1]

    @staticmethod
    def fracao_para_percentual(valor):
        '''Converte a fracao usada no frontend (0.015) no percentual gravado (1.50)'''
        return round(float(valor) * 100, 2)

    @staticmethod
    def percentual_para_fracao(valor):
        '''Converte o percentual gravado (1.50) na fracao usada no frontend (0.015)'''
        return round(float(valor) / 100, 6)

    @staticmethod
    def __sem_acento(texto):
        '''Metodo privado que remove acentos para comparar nomes de mes'''
        normalizado = unicodedata.normalize('NFKD', texto)

        return ''.join(caractere for caractere in normalizado if not unicodedata.combining(caractere))
