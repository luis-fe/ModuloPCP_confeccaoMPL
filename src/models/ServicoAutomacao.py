from datetime import datetime

import pytz

from src.connection import ConexaoPostgre
import pandas as pd


class ServicoAutomacao():

    def __init__(self, data_hora = '', rotina = ''):

        self.data_hora = data_hora
        self.data_hora_atual = self.obterHoraAtual()
        self.rotina = rotina




    def obtendo_historico_automacao(self):

        sql = """
                select
                    rotina,
                    data_hora
                from
                    "PCP".pcp."ControleAutomacao"
        """


        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn)

        return consulta

    def obtendo_historico_automacao_rotina(self):
        sql = f"""
                select
                    rotina,
                    data_hora
                from
                    "PCP".pcp."ControleAutomacao"
                where 
                    rotina = '{self.rotina}'
                order by data_hora desc
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql, conn)

        return consulta


    def obtendo_ultima_atualizacao_rotina(self):

        consulta = self.obtendo_historico_automacao_rotina()

        if consulta.empty:
            ultimo = '2000-01-01 00:00:00'
        else:

            ultimo = consulta['data_hora'][0]

        return ultimo


    def obtentendo_intervalo_atualizacao_rotina(self):
        # Converte as strings para objetos datetime
        data1_obj = datetime.strptime(self.obterHoraAtual(), "%Y-%m-%d %H:%M:%S")

        data2_obj = datetime.strptime(self.obtendo_ultima_atualizacao_rotina(), "%Y-%m-%d  %H:%M:%S")

        # Calcula a diferença entre as datas
        diferenca = data1_obj - data2_obj

        # Obtém a diferença total em segundos
        diferenca_total_segundos = diferenca.total_seconds()
        intervalo = float(diferenca_total_segundos)


        return intervalo



    def inserindo_automacao(self):

        insert = """
        insert into pcp."ControleAutomacao" ("data_hora", "rotina" ) values ( %s , %s )
        """



        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.data_hora, self.rotina))
                conn.commit()

    def obterHoraAtual(self):



        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora





