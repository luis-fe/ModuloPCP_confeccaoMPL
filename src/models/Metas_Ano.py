from src.connection import ConexaoPostgre
import pandas as pd
import datetime
import pytz


"""Classe responsavel pela gestao das metas anuais """
class Metas_ano():

    def __init__(self, codEmpresa = '', ano = '', meta = '', mes = '', codUsuario = '', nomeUsuario = ''):

        self.codEmpresa = codEmpresa
        self.ano = ano
        self.meta = meta
        self.mes = mes
        self.codUsuario = codUsuario
        self.nomeUsuario = nomeUsuario

    def get_metas(self):
        '''metodo utilizado para obter as metas atribuidas por empresa'''

        conn = ConexaoPostgre.conexaoEngine()
        if self.codEmpresa != 'Todas':
            consulta = pd.read_sql("""
                                        select
                                            mes as "Mês", 
                                            meta 
                                        from 
                                            "DashbordTV".metas 
                                        where 
                                            empresa = %s 
                                            and ano = %s   
                                        order by 
                                            mes 
                                            """
                                   , conn, params=(self.codEmpresa, self.ano))
        else:
            consulta = pd.read_sql("""
                                    select 
                                        mes as "Mês", 
                                        meta 
                                    from 
                                        "PCP"."DashbordTV".metas 
                                    where 
                                        ano = %s  
                                    order by 
                                        mes
                                   """, conn, params=(self.ano,))
            consulta = consulta.groupby('Mês').agg({
                'Mês': 'first',
                'meta': 'sum'
            })

        consulta['meta acum.'] = consulta['meta'].cumsum()

        consulta.fillna('-', inplace=True)
        metaTotal = consulta['meta'].sum()
        consulta['meses'] = consulta['Mês']

        return consulta, metaTotal



    def update_meta_empresa(self):
        '''Metodo responsavel por gravar as alteracoes de uma determinada meta'''

        update = """
            update 
                "DashbordTV".metas
            set 
                meta = %s
            where 
                empresa = %s 
                and ano = %s   
                and mes = %s
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(update,(self.meta, self.codEmpresa, self.ano, self.mes))
                conn.commit()

    def inserir_metas_empresa(self):

        insert = """
                insert into 
                    "DashbordTV".metas 
                        ( empresa, ano, mes, meta) 
                    values 
                        ( %s, %s, %s, %s )
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert, (self.codEmpresa, self.ano, self.mes, self.meta))
                conn.commit()


    def obter_meta_empresa_ano_especifico(self):

        consulta = """
                  select
                        mes as "Mês", 
                        meta 
                    from 
                        "DashbordTV".metas 
                    where 
                        empresa = %s 
                        and ano = %s  
                        and mes = %s 
                    """
        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.codEmpresa, self.ano, self.mes))

        return consulta
    def controle_alteracoes(self):
        """Metodo utilizado para o histotico de controle de alteracoes em metas"""

        insert = """insert into "DashbordTV"."controleMudancasMetas" (
        "codUsuario",
            "nomeUsuario",
            "dataHora",
            "codEmpresa",
            "ano",
            "mes",
            "metaAnterior",
            "metaNova"
        ) 
        values ( %s, %s, %s, %s, %s, %s, %s, %s)
        """

        # Obtendo a MetaAnterior

        metaAnterior = self.obter_meta_empresa_ano_especifico()
        metaAnterior = metaAnterior['meta'][0]

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert, (self.codUsuario, self.nomeUsuario, self.__obterHoraAtual(), self.codEmpresa, self.ano, self.mes, metaAnterior, self.meta ))
                conn.commit()


    def consultaAlteracoes(self):
        '''Metodo que consulta as alteracoes'''

        consulta = """
        select
            "codUsuario",
            "nomeUsuario",
            "dataHora",
            "codEmpresa",
            "ano",
            "mes",
            "metaAnterior",
            "metaNova"
        from
            "DashbordTV"."controleMudancasMetas"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta



    def __obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')


        return hora_str