import pandas as pd
from src.connection import ConexaoPostgre
import requests
import pytz
import datetime

class DashboardTV():
        ''''Classe responsavel pelo gerenciamento do Dashboard da TV '''

        def __init__(self, codEmpresa = '', codAno = '', meses = '', metas_valores = '',
                     usuario = '', nome = '', senha = ''
                     ):

            self.codEmpresa = str(codEmpresa)
            self.codAno = str(codAno)
            self.meses = meses
            self.metas_valores = metas_valores

            self.usuario = usuario
            self.nome = nome
            self.senha = senha


        def get_metas_cadastradas_ano_empresa(self):
            '''Metodo que obtem as metas cadastradas para um determinado ano '''

            dados = [
                { "mes": "01-Janeiro"},
                { "mes": "02-Fevereiro"},
                { "mes": "03-Março"},
                {"mes": "04-Abril"},
                { "mes": "05-Maio"},
                {"mes": "06-Junho"},
                {"mes": "07-Julho"},
                {"mes": "08-Agosto"},
                {"mes": "09-Setembro"},
                {"mes": "10-Outubro"},
                {"mes": "11-Novembro"},
                {"mes": "12-Dezembro"}
            ]

            # DataFrame somente com a coluna 'mes'
            mes = pd.DataFrame(dados)[["mes"]]


            sql = """
            select
                mes, meta
            from
                "PCP"."DashbordTV".metas m
            where 
                m.ano = %s 
                and m.empresa = %s
            order by mes 
            """
            conn = ConexaoPostgre.conexaoEngine()
            consulta = pd.read_sql(sql,conn, params=(self.codAno, self.codEmpresa))

            consulta = pd.merge(mes, consulta, on='mes', how='left')
            consulta.fillna('R$0,00',inplace=True)

            consulta["meta"] = consulta["meta"].apply(
                lambda x: f"R$ {x:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

            return consulta

        def consulta_meta_ano_mes_empresa(self, mes):
            '''Metodo que consulta empresa, ano, mes '''

            sql = """
                            select
                mes, meta, ano
            from
                "PCP"."DashbordTV".metas m
            where 
                m.ano = %s 
                and m.empresa = %s
                and m.mes = %s
            order by mes 
            """

            conn = ConexaoPostgre.conexaoEngine()
            consulta = pd.read_sql(sql,conn, params=(self.codAno, self.codEmpresa, mes))

            return consulta

        def post_metas_empresa_ano(self):
            '''Metodo que grava as metas dos meses '''

            # Ajusta nomes dos meses (ex: "01-Janeiro")
            # Nota: Certifique-se que self.meses tenha apenas os nomes ("Janeiro", etc) inicialmente
            lista_meses_formatada = [f"{i + 1:02d}-{nome}" for i, nome in enumerate(self.meses)]

            # CORREÇÃO 1: Usar zip() para iterar duas listas simultaneamente
            for mes, meta_raw in zip(lista_meses_formatada, self.metas_valores):

                # CORREÇÃO 2: Tratamento do valor (R$ string -> int)
                try:
                    # Se for string, limpa. Se já for número, mantém.
                    if isinstance(meta_raw, str):
                        # Remove pontos de milhar, R$, espaços e troca vírgula decimal por ponto
                        # Ex: "R$ 1.200,50" -> "1200.50"
                        val_limpo = meta_raw.replace("R$", "").replace(" ", "").replace(".", "").replace(",", ".")
                        meta_final = int(float(val_limpo))
                    else:
                        meta_final = int(meta_raw)
                except ValueError:
                    meta_final = 0  # Valor padrão caso venha lixo

                # Verifica se já existe (Recomendação: O ideal seria fazer isso fora do loop para performance, mas mantive sua lógica)
                verifica = self.consulta_meta_ano_mes_empresa(mes)

                with ConexaoPostgre.conexaoInsercao() as conn:
                    with conn.cursor() as curr:

                        if verifica.empty:
                            # CORREÇÃO 3: Adicionado o 5º %s para o ID
                            insert = """
                            insert into "PCP"."DashbordTV".metas
                            (ano, mes, meta, empresa, id) values ( %s, %s, %s, %s, %s)
                            """
                            id_gerado = f"{self.codAno}-{mes}"

                            curr.execute(insert, (self.codAno, mes, meta_final, self.codEmpresa, id_gerado))
                        else:
                            update = """
                            update 
                                "PCP"."DashbordTV".metas
                            set 
                                meta = %s
                            where 
                                empresa = %s
                                and ano = %s
                                and mes = %s   
                            """
                            curr.execute(update, (meta_final, self.codEmpresa, self.codAno, mes))

                        conn.commit()

            return pd.DataFrame([{'Mensagem': 'Dados gravados com sucesso', 'status': True}])



        def usuario_autentificar(self):
            '''metodo para criar usuario'''

            usuarioInformacao = self.obter_informacao_autentificacao()

            if usuarioInformacao.empty:

                return pd.DataFrame([{'Mensagem':"Cadastar Nova Senha",'status':False}])

            else:
                print(usuarioInformacao)

                if str(self.senha) == usuarioInformacao['senha'][0]:
                    return pd.DataFrame([{'Mensagem': "Autentificado com sucesso", 'status': True}])
                else:
                    return pd.DataFrame([{'Mensagem': "Senhas nao Confere", 'status': False}])



        def criar_editar_senha(self):

            verificar = self.obter_informacao_autentificacao()

            if verificar.empty:
                insert = """
                insert into 
                    "PCP"."DashbordTV".autentificacao
                (matricula, nome, "senha")
                values 
                (%s, %s, %s)
                """

                with ConexaoPostgre.conexaoInsercao() as conn:
                    with conn.cursor() as curr:

                        curr.execute(insert,(self.usuario, self.nome, self.usuario))
                        conn.commit()

                return pd.DataFrame([{'Mensagem':'usuario salvo com sucesso', 'status':True}])
            else:

                update = """
                update  "PCP"."DashbordTV".autentificacao
                set senha = %s
                where matricula = %s
                """

                with ConexaoPostgre.conexaoInsercao() as conn:
                    with conn.cursor() as curr:
                        curr.execute(update, (self.senha, self.usuario))
                        conn.commit()

                return pd.DataFrame([{'Mensagem': 'usuario salvo com sucesso', 'status': True}])


        def obter_informacao_autentificacao(self):

            select = """
            select 
                matricula,
                nome,
                senha
             from "PCP"."DashbordTV".autentificacao
            where  
                matricula = %s
            """

            conn = ConexaoPostgre.conexaoEngine()
            consulta = pd.read_sql(select,conn,params=(self.usuario,))

            return consulta


        def devolver_nome_usuario(self):


            usuarioDados = self.get_colaboradores_api()
            usuarioDados = usuarioDados[["nome"]]

            return usuarioDados


        def get_colaboradores_api(self):
            '''Consome API de colaboradores e retorna DataFrame filtrado pela empresa'''

            url = "http://10.162.0.202:3001/api/colaboradores"

            try:
                # 1. Faz a requisição
                response = requests.get(url, timeout=10)  # Timeout evita travamento eterno
                response.raise_for_status()  # Levanta erro se der 404 ou 500

                # 2. Transforma o JSON em DataFrame
                dados = response.json()
                df = pd.DataFrame(dados)

                if df.empty:
                    return pd.DataFrame()

                # 3. Tratamento de Data (Opcional, mas recomendado para visualização)
                # Converte de string ISO para objeto datetime do Pandas

                if 'empresa' in df.columns and self.codEmpresa:
                    df = df[df['empresa'] == int(self.codEmpresa)]

                df['id'] = df['id'].astype(str)
                df = df[df['id']==self.usuario]

                return df

            except requests.exceptions.RequestException as e:
                print(f"Erro ao conectar na API: {e}")
                return pd.DataFrame()  # Retorna vazio em caso de erro para não quebrar o sistema
            except Exception as e:
                print(f"Erro ao processar dados: {e}")
                return pd.DataFrame()



        def gravar_usuario_alteracao_meta(self):
            '''Metodo que grava o usuario que esta alterando as metas '''

            insert = """
                insert into 
                    "PCP"."DashbordTV"."historicoAltMetas"
                (matricula, empresa, ano, "dataHora")
                    values
                (%s, %s, %s, %s)
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert,(self.usuario, self.codEmpresa, self.codAno, self.__obterHoraAtual()))
                    conn.commit()


        def get_ultima_alteracao(self):
            '''Metodo que devolve a ultima alteracao'''

            consulta = """
            select 
                aut.matricula,
                aut.nome,
                "dataHora"
            from 
                "PCP"."DashbordTV"."historicoAltMetas" h
            inner join
                "PCP"."DashbordTV".autentificacao aut
                on aut.matricula = h.matricula
            where
                empresa = %s
                and ano = %s
            order by 
                "dataHora" desc
            """

            conn = ConexaoPostgre.conexaoEngine()
            consulta = pd.read_sql(consulta, conn, params=(self.codEmpresa, self.codAno,))

            consulta = consulta.loc[0:0]

            return consulta


        def __obterHoraAtual(self):
            fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
            agora = datetime.datetime.now(fuso_horario)
            hora_str = agora.strftime('%d/%m/%a %H:%M:%S')
            dia = agora.strftime('%Y-%m-%d')
            return hora_str











