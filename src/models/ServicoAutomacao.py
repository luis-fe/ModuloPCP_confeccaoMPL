from datetime import datetime
import pytz
from src.connection import ConexaoPostgre
import pandas as pd


class ServicoAutomacao():
    '''Classe responsabel por gerenciar o serviço de Automacao '''
    def __init__(self, idServico ='', descricaoServico = '', periodoInicial = "",periodoFinal = ""):

        # 1 - No processo de criacao verifica a dataHora Atual do Sistema Operacional
        self.data_hora_atual = self.obterHoraAtual()

        self.idServico = idServico
        self.descricaoServico = descricaoServico
        self.periodoInicial = periodoInicial
        self.periodoFinal = periodoFinal




    def obtendo_historico_automacao(self):
        '''Metodo Publico para obter o historico dos serviços de automacao no periodo'''

        sql = """
                select
                    c."idServico",
                    "dataAtualizacao",
                    s."descricaoServico" 
                from
                    pcp."ControleAutomacao" c
                inner join 
                    pcp."ServicoAutomacao" s
                on 
                    s."idServico" = c."idServico"
                order by "dataAtualizacao" desc
        """


        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn)

        return consulta

    def obtendo_historico_automacao_servico(self):
        '''Metodo Publico que obtem o historico  de movimentacao serviço  especifico, em um determinado periodo'''
        sql = f"""
                select
                    c."idServico",
                    "dataAtualizacao",
                    s."descricaoServico" 
                from
                    pcp."ControleAutomacao" c
                inner join 
                    pcp."ServicoAutomacao" s
                on 
                    s."idServico" = c."idServico"
                where 
                    "descricaoServico" = '{self.descricaoServico}'
                order by "dataAtualizacao" desc
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql, conn)

        return consulta


    def obtendo_ultima_atualizacao_(self):
        """Metodo publico que obtem "A ULTIMA" movimentacao do serviço em especifico """


        sql = """
            SELECT 
                "idServico", 
                "dataAtualizacao", 
                "descricaoServico"
            FROM (
                SELECT
                    c."idServico",
                    c."dataAtualizacao",
                    s."descricaoServico",
                    ROW_NUMBER() OVER (
                        PARTITION BY c."idServico" 
                        ORDER BY c."dataAtualizacao" DESC
                    ) as rn
                FROM
                    pcp."ControleAutomacao" c
                INNER JOIN 
                    pcp."ServicoAutomacao" s ON c."idServico" = s."idServico"
            ) t
            WHERE rn = 1
            ORDER BY "dataAtualizacao" DESC;
        """


        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn)

        # 2. Converte para datetime (caso ainda não seja)
        # Isso é essencial para habilitar as funções de data
        consulta['dataAtualizacao'] = pd.to_datetime(consulta['dataAtualizacao'])

        # 3. Cria a coluna DATA no formato BR (Dia/Mês/Ano)
        consulta['data'] = consulta['dataAtualizacao'].dt.strftime('%d/%m/%Y')

        # 4. Cria a coluna HORA (Hora:Minuto:Segundo)
        consulta['hora'] = consulta['dataAtualizacao'].dt.strftime('%H:%M:%S')

        return consulta


    def obtendo_ultima_atualizacao_rotina(self):
        """Metodo publico que obtem "A ULTIMA" movimentacao do serviço em especifico """

        consulta = self.obtendo_historico_automacao_servico()


        if consulta.empty:
            ultimo = '2000-01-01 00:00:00'

        else:
            if consulta['dataAtualizacao'][0] == None:
                ultimo = '2000-01-01 00:00:00'
            else:

                ultimo = consulta['dataAtualizacao'][0]

        return ultimo



    def obtentendo_intervalo_atualizacao_servico(self):
        '''Metodo publico que obtem o Intervalo entre a data atual x ultima atualizacao de um Servuco especifico e
        return: interval - minutos
        '''

        # Converte as strings para objetos datetime
        data1_obj = datetime.strptime(self.obterHoraAtual(), "%Y-%m-%d %H:%M:%S")

        data2_obj = datetime.strptime(self.obtendo_ultima_atualizacao_rotina(), "%Y-%m-%d  %H:%M:%S")

        # Calcula a diferença entre as datas
        diferenca = data1_obj - data2_obj

        # Obtém a diferença total em segundos
        diferenca_total_segundos = diferenca.total_seconds()
        intervalo = float(diferenca_total_segundos)


        return intervalo



    def inserindo_automacao(self, dataHora):
        '''Metodo publico que inseri a automacao'''

        insert = """
        insert into pcp."ControleAutomacao" 
        (
	        "idServico",
	        "dataAtualizacao",
	        "statusAutomacao",
	        "mediaUsoRam"
        ) values ( 
            %s , 
            %s ,
            %s ,
            %s  
        )
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.idServico, dataHora, 'Iniciado',''))
                conn.commit()


    def update_controle_automacao(self, descricaoStatus,dataHora):
        '''Metodo publico que atualiza o status do controle de automacao'''


        update = """
        update 
            pcp."ControleAutomacao" 
        set 
            "statusAutomacao" = %s, "dataAtualizacao" = %s
        where 
            "statusAutomacao" not like 'Finalizado%%'
            and "idServico" = %s
        
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(update,(descricaoStatus, dataHora, self.idServico))
                conn.commit()



    def obterHoraAtual(self):
        """Metodo publico que obtem a data e hora da ultima atualizacao do sistema Operacional """


        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora


    def exluir_historico_antes_quarentena(self):
        '''Metodo publico que exluir o historico dos serviços da data anterior a 40 dias do dia atual,
        para economia de espaço no banco de dados do projeto
        '''
        exclusao = """
        delete FROM
                 "PCP".pcp."ControleAutomacao"
        WHERE
        "dataAtualizacao"::Date < (NOW() - INTERVAL '40 days');
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(exclusao,)
                conn.commit()





