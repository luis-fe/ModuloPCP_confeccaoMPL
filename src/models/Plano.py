from datetime import datetime, timedelta
import pandas as pd
import pytz
from src.connection import ConexaoPostgre
from src.models import Plano_Lote

class Plano():
    '''
    Classe criada para o "Plano" do PCP que é um conjunto de parametrizacoes para se fazer um planejamento.
    '''

    def __init__(self, codPlano = None, descricaoPlano= None, iniVendas = None , fimVendas = None, iniFat = None, fimFat = None,
                 usuarioGerador = None, codEmpresa = '1' ):
        '''
        Definicao do construtor: atributos do plano
        '''
        self.codPlano = codPlano
        self.descricaoPlano  = descricaoPlano
        self.iniVendas = iniVendas
        self.fimVendas = fimVendas
        self.iniFat = iniFat
        self.fimFat = fimFat
        self.usuarioGerador = usuarioGerador
        self.codEmpresa = codEmpresa

    def inserirNovoPlano(self):
        '''
        Inserindo um novo plano

        :returns:
            status do plano no formado DATAFRAME-pandas
        '''

        # Validando se o Plano ja existe
        validador = self.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if not validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': 'O Plano ja existe'}])

        else:

            insert = """INSERT INTO pcp."Plano" ("codEmpresa", "codigo","descricaoPlano","inicioVenda","FimVenda","inicoFat", "finalFat", "usuarioGerador","dataGeracao") 
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s ) """

            data = self.obterdiaAtual()
            conn = ConexaoPostgre.conexaoInsercao()
            cur = conn.cursor()
            cur.execute(insert,
                        (self.codEmpresa, self.codPlano, self.descricaoPlano, self.iniVendas, self.fimVendas, self.iniFat, self.fimFat, self.usuarioGerador, str(data),))
            conn.commit()
            cur.close()
            conn.close()

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Novo Plano Criado com sucesso !'}])


    def consultarPlano(self):
        '''
        Metoto que busca todos os planos cadastrados no Sistema do PCP
        :return:
        DataFrame (em pandas) com todos os planos
        '''
        conn = ConexaoPostgre.conexaoEngine()
        planos = pd.read_sql("""
                    SELECT 
                        * 
                    FROM 
                        pcp."Plano" 
                    WHERE
                        "codEmpresa" = %s
                    ORDER BY codigo ASC;""", conn, params=(self.codEmpresa,))

        return planos


    def obterdiaAtual(self):
        '''
        Método para obter a data atual do dia
        :return:
            'data de hoje no formato - %d/%m/%Y'
        '''
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%M-%D')
        return agora


    def alterarPlano(self):
        '''Método criado para alterar um determinado Plano Criado'''
        validador = self.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': 'O Plano Informado nao existe'}])
        else:
            descricaoPlanoAtual = validador['descricaoPlano'][0]
            if descricaoPlanoAtual == self.descricaoPlano or self.descricaoPlano == '-':
                self.descricaoPlano = descricaoPlanoAtual


            iniVendasAtual = validador['inicioVenda'][0]
            if iniVendasAtual == self.iniVendas or self.iniVendas == '-':
                self.iniVendas = iniVendasAtual


            FimVendaAtual = validador['FimVenda'][0]
            if FimVendaAtual == self.fimVendas or self.fimVendas == '-':
                self.fimVendas = FimVendaAtual


            inicoFatAtual = validador['inicoFat'][0]
            if inicoFatAtual == self.iniFat or self.iniFat == '-':
                self.iniFat = inicoFatAtual

            finalFatAtual = validador['finalFat'][0]
            if finalFatAtual == self.fimFat or self.fimFat == '-':
                self.fimFat = finalFatAtual

            update = """update pcp."Plano"  set "descricaoPlano" = %s , "inicioVenda" = %s , "FimVenda" = %s , "inicoFat" = %s , "finalFat" = %s
            where "codigo" = %s and "codEmpresa" = %s
            """

            conn = ConexaoPostgre.conexaoInsercao()
            cur = conn.cursor()
            cur.execute(update, (self.descricaoPlano, self.iniVendas, self.fimVendas, self.iniFat, self.fimFat, self.codPlano ,self.codEmpresa))
            conn.commit()
            cur.close()
            conn.close()
            return pd.DataFrame([{'Status': True, 'Mensagem': 'O Plano foi alterado com sucesso !'}])



    def obterPlanos(self):
        '''Metodo que obtem os Planos cadatrados '''
        conn = ConexaoPostgre.conexaoEngine()
        planos = pd.read_sql("""
                SELECT 
                    * 
                FROM 
                    pcp."Plano"
                where
                    "codEmpresa" = %s 
                ORDER BY 
                    codigo ASC;""", conn, params=(self.codEmpresa,))
        planos.rename(
            columns={'codigo': '01- Codigo Plano', 'descricaoPlano': '02- Descricao do Plano',
                     'inicioVenda': '03- Inicio Venda',
                     'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento",
                     "finalFat": "06- Final Faturamento",
                     'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
            inplace=True)
        planos.fillna('-', inplace=True)

        sqlLoteporPlano = """
        select
            plano as "01- Codigo Plano",
            lote,
            nomelote
        from
            pcp."LoteporPlano"
        where 
            "codEmpresa" = %s 
        """

        sqlTipoNotasPlano = """select "tipo nota"||'-'||nome as "tipoNota" , plano as "01- Codigo Plano"  from pcp."tipoNotaporPlano" tnp """

        lotes = pd.read_sql(sqlLoteporPlano, conn, params=(self.codEmpresa,))
        TipoNotas = pd.read_sql(sqlTipoNotasPlano, conn)

        lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

        merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')
        merged = pd.merge(merged, TipoNotas, on='01- Codigo Plano', how='left')

        # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
        grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                                  '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador',
                                  '08- Data Geracao']).agg({
            'lote': lambda x: list(x.dropna().astype(str).unique()),
            'nomelote': lambda x: list(x.dropna().astype(str).unique()),
            'tipoNota': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()

        result = []
        for index, row in grouped.iterrows():
            entry = {
                '01- Codigo Plano': row['01- Codigo Plano'],
                '02- Descricao do Plano': row['02- Descricao do Plano'],
                '03- Inicio Venda': row['03- Inicio Venda'],
                '04- Final Venda': row['04- Final Venda'],
                '05- Inicio Faturamento': row['05- Inicio Faturamento'],
                '06- Final Faturamento': row['06- Final Faturamento'],
                '07- Usuario Gerador': row['07- Usuario Gerador'],
                '08- Data Geracao': row['08- Data Geracao'],
                '09- lotes': row['lote'],
                '10- nomelote': row['nomelote'],
                '11-TipoNotas': row['tipoNota']
            }
            result.append(entry)

        return result


    def obterPlanosPlano(self):
        '''Metodo que obtem um determinado plano '''

        conn = ConexaoPostgre.conexaoEngine()
        planos = pd.read_sql('SELECT * FROM pcp."Plano" where "codEmpresa" = %s ORDER BY codigo ASC;', conn, params=(self.codEmpresa,))
        planos.rename(
            columns={'codigo': '01- Codigo Plano', 'descricaoPlano': '02- Descricao do Plano',
                     'inicioVenda': '03- Inicio Venda',
                     'FimVenda': '04- Final Venda', "inicoFat": "05- Inicio Faturamento",
                     "finalFat": "06- Final Faturamento",
                     'usuarioGerador': '07- Usuario Gerador', 'dataGeracao': '08- Data Geracao'},
            inplace=True)
        planos.fillna('-', inplace=True)
        planos = planos[planos['01- Codigo Plano'] == self.codPlano].reset_index()
        sqlLoteporPlano = """
        select
            plano as "01- Codigo Plano",
            lote,
            nomelote
        from
            pcp."LoteporPlano"
        where 
            "codEmpresa" = %s
        """

        sqlTipoNotasPlano = """select "tipo nota"||'-'||nome as "tipoNota" , plano as "01- Codigo Plano"  from pcp."tipoNotaporPlano" tnp """

        lotes = pd.read_sql(sqlLoteporPlano, conn, params=(self.codEmpresa,))
        TipoNotas = pd.read_sql(sqlTipoNotasPlano, conn)

        lotes['01- Codigo Plano'] = lotes['01- Codigo Plano'].astype(str)

        merged = pd.merge(planos, lotes, on='01- Codigo Plano', how='left')
        merged = pd.merge(merged, TipoNotas, on='01- Codigo Plano', how='left')

        # Agrupa mantendo todas as colunas do DataFrame planos e transforma lotes e nomelote em arrays
        grouped = merged.groupby(['01- Codigo Plano', '02- Descricao do Plano', '03- Inicio Venda', '04- Final Venda',
                                  '05- Inicio Faturamento', '06- Final Faturamento', '07- Usuario Gerador',
                                  '08- Data Geracao']).agg({
            'lote': lambda x: list(x.dropna().astype(str).unique()),
            'nomelote': lambda x: list(x.dropna().astype(str).unique()),
            'tipoNota': lambda x: list(x.dropna().astype(str).unique())
        }).reset_index()

        result = []
        for index, row in grouped.iterrows():
            entry = {
                '01- Codigo Plano': row['01- Codigo Plano'],
                '02- Descricao do Plano': row['02- Descricao do Plano'],
                '03- Inicio Venda': row['03- Inicio Venda'],
                '04- Final Venda': row['04- Final Venda'],
                '05- Inicio Faturamento': row['05- Inicio Faturamento'],
                '06- Final Faturamento': row['06- Final Faturamento'],
                '07- Usuario Gerador': row['07- Usuario Gerador'],
                '08- Data Geracao': row['08- Data Geracao'],
                '09- lotes': row['lote'],
                '10- nomelote': row['nomelote'],
                '11-TipoNotas': row['tipoNota']
            }
            result.append(entry)

        return result


    def excluirPlano(self):
        '''Metodo que exclui um determinado Plano'''

        # 1 - Verificando se existe vinculo do Plano a um planejamento ABC

        vinculoABC = self.consultaVinculoABC_Plano()

        vinculoLote = Plano_Lote.Plano_Lote().consultaVinculoLotes_Plano()

        if not vinculoABC.empty:
            return pd.DataFrame([{'status':False, 'mensagem':'Existe vinculo do plano a um ABC'}])

        if not vinculoLote.empty:
            return pd.DataFrame([{'status':False, 'mensagem':'Existe vinculo do plano a Lotes '}])





    def consultaVinculoABC_Plano(self):
        '''Metodo que consulta a estrutura ABC vinculado ao plano '''

        sql = """
        select 
            * 
        from 
            pcp."Plano" p 
        inner join 
            pcp."Plano_ABC"  pa 
            on pa."codPlano" = p.codigo 
        where p.codigo = %s
        """


        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        return consulta



    def pesquisarInicioFimVendas(self):
        '''metodo que pesquisa o inicio e o fim das vendas baseado no codPlano'''

        sql = """
        select 
            "inicioVenda","FimVenda"
        from
            pcp."Plano"
        where
            "codigo" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        if not consulta.empty:

            inicioVenda = consulta['inicioVenda'][0]
            FimVenda = consulta['FimVenda'][0]

            return inicioVenda, FimVenda

        else:
            return '-', '-'

    def pesquisarInicioFimFat(self):
        '''metodo que pesquisa o inicio e o fim das vendas passeado no codPlano'''

        sql = """
        select 
            "inicoFat","finalFat"
        from
            pcp."Plano"
        where
            "codigo" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano,))

        if not consulta.empty:

            inicoFat = consulta['inicoFat'][0]
            finalFat = consulta['finalFat'][0]

            return inicoFat, finalFat

        else:
            return '-', '-'

    def obterNumeroSemanasVendas(self):
            '''Metodo que obtem o numero de semanas de vendas do Plano
            Calcula o número de semanas entre duas datas, considerando:
            - A semana começa na segunda-feira.
            - Se a data inicial não for uma segunda-feira, considera a primeira semana começando na data inicial.

            Parâmetros:
                ini (str): Data inicial no formato 'YYYY-MM-DD'.
                fim (str): Data final no formato 'YYYY-MM-DD'.

            Retorna:
                int: Número de semanas entre as duas datas.
            '''

            self.iniVendas, self.fimVendas = self.pesquisarInicioFimVendas()

            if self.iniVendas == '-':
                return 0
            else:

                data_ini = datetime.strptime(self.iniVendas, '%Y-%m-%d')
                data_fim = datetime.strptime(self.fimVendas, '%Y-%m-%d')

                if data_ini > data_fim:
                    raise ValueError("A data inicial deve ser anterior ou igual à data final.")

                # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
                if data_ini.weekday() != 0:  # 0 representa segunda-feira
                    proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
                else:
                    proxima_segunda = data_ini

                # Calcular o número de semanas completas a partir da próxima segunda-feira
                semanas_completas = (data_fim - proxima_segunda).days // 7

                # Verificar se existe uma semana parcial no final
                dias_restantes = (data_fim - proxima_segunda).days % 7
                semana_inicial_parcial = 1 if data_ini.weekday() != 0 else 0
                semana_final_parcial = 1 if dias_restantes > 0 else 0

                return semanas_completas + semana_inicial_parcial + semana_final_parcial


    def obterSemanaAtualFat(self):
        '''Calcula em qual semana está o dia atual dentro do intervalo de vendas.
        Caso o dia atual esteja fora do intervalo (após a data final), retorna "finalizado".

        Retorna:
            int ou str: Número da semana atual ou "finalizado".
        '''
        self.iniFat, self.fimFat = self.pesquisarInicioFimFat()

        if self.iniFat == '-':
            return "finalizado"

        data_ini = datetime.strptime(self.iniFat, '%Y-%m-%d')
        data_fim = datetime.strptime(self.fimFat, '%Y-%m-%d')
        hoje = datetime.today()

        if data_ini > data_fim:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        if hoje > data_fim:
            return "finalizado"

        # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
        if data_ini.weekday() != 0:  # 0 representa segunda-feira
            proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
        else:
            proxima_segunda = data_ini

        # Calcular a diferença de semanas entre a data inicial ajustada e hoje
        semanas_completas = (hoje - proxima_segunda).days // 7

        # Verificar se hoje está na primeira semana parcial
        semana_inicial_parcial = 1 if hoje < proxima_segunda and hoje >= data_ini else 0

        # Retornar o número da semana atual
        return semanas_completas + semana_inicial_parcial + 1



    def obterSemanaAtual(self):
        '''Calcula em qual semana está o dia atual dentro do intervalo de vendas.
        Caso o dia atual esteja fora do intervalo (após a data final), retorna "finalizado".

        Retorna:
            int ou str: Número da semana atual ou "finalizado".
        '''
        self.iniVendas, self.fimVendas = self.pesquisarInicioFimVendas()

        if self.iniVendas == '-':
            return "finalizado"

        data_ini = datetime.strptime(self.iniVendas, '%Y-%m-%d')
        data_fim = datetime.strptime(self.fimVendas, '%Y-%m-%d')
        hoje = datetime.today()

        if data_ini > data_fim:
            raise ValueError("A data inicial deve ser anterior ou igual à data final.")

        if hoje > data_fim:
            return "finalizado"

        # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
        if data_ini.weekday() != 0:  # 0 representa segunda-feira
            proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
        else:
            proxima_segunda = data_ini

        # Calcular a diferença de semanas entre a data inicial ajustada e hoje
        semanas_completas = (hoje - proxima_segunda).days // 7

        # Verificar se hoje está na primeira semana parcial
        semana_inicial_parcial = 1 if hoje < proxima_segunda and hoje >= data_ini else 0

        # Retornar o número da semana atual
        return semanas_completas + semana_inicial_parcial + 1


    def obterNumeroSemanasFaturamento(self):
            '''Metodo que obtem o numero de semanas de faturamento do Plano
            Calcula o número de semanas entre duas datas, considerando:
            - A semana começa na segunda-feira.
            - Se a data inicial não for uma segunda-feira, considera a primeira semana começando na data inicial.

            Parâmetros:
                ini (str): Data inicial no formato 'YYYY-MM-DD'.
                fim (str): Data final no formato 'YYYY-MM-DD'.

            Retorna:
                int: Número de semanas entre as duas datas.
            '''

            self.iniFat, self.fimFat = self.pesquisarInicioFimFat()

            if self.iniFat == '-':
                return 0
            else:

                data_ini = datetime.strptime(self.iniFat, '%Y-%m-%d')
                data_fim = datetime.strptime(self.fimFat, '%Y-%m-%d')

                if data_ini > data_fim:
                    raise ValueError("A data inicial deve ser anterior ou igual à data final.")

                # Ajustar para a próxima segunda-feira, se a data inicial não for segunda
                if data_ini.weekday() != 0:  # 0 representa segunda-feira
                    proxima_segunda = data_ini + timedelta(days=(7 - data_ini.weekday()))
                else:
                    proxima_segunda = data_ini

                # Calcular o número de semanas completas a partir da próxima segunda-feira
                semanas_completas = (data_fim - proxima_segunda).days // 7

                # Verificar se existe uma semana parcial no final
                dias_restantes = (data_fim - proxima_segunda).days % 7
                semana_inicial_parcial = 1 if data_ini.weekday() != 0 else 0
                semana_final_parcial = 1 if dias_restantes > 0 else 0

                return semanas_completas + semana_inicial_parcial + semana_final_parcial




