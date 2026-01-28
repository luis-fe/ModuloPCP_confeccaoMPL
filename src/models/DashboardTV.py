import os
import tempfile
from dotenv import load_dotenv, dotenv_values
import pandas as pd
from src.configApp import configApp
from src.connection import ConexaoPostgre
import requests
import pytz
from src.models import Pedidos_CSW
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
class DashboardTV():
        ''''Classe responsavel pelo gerenciamento do Dashboard da TV '''

        def __init__(self, codEmpresa = '', codAno = '', meses = '', metas_valores = '',
                     usuario = '', nome = '', senha = '', codTipoNota = '', dataInicio = '', dataFim = ''
                     ):

            self.codEmpresa = str(codEmpresa)
            self.codAno = str(codAno)
            self.meses = meses
            self.metas_valores = metas_valores

            self.usuario = usuario
            self.nome = nome
            self.senha = senha
            self.codTipoNota = codTipoNota
            self.dataInicio = dataInicio
            self.dataFim = dataFim


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


            conn = ConexaoPostgre.conexaoEngine()


            if self.codEmpresa == 'Todas':
                sql = """
                select
                    mes, sum(meta) as meta
                from
                    "PCP"."DashbordTV".metas m
                where 
                    m.ano = %s 
                    and m.empresa in ('1','4')
                group by mes
                order by mes 
                """

                consulta = pd.read_sql(sql,conn, params=(self.codAno,))


            else:


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
            # 🔹 converte para datetime (caso venha como string)
            consulta["dataHora"] = pd.to_datetime(consulta["dataHora"])

            # 🔹 converte para string no formato brasileiro
            consulta["dataHora"] = consulta["dataHora"].dt.strftime("%d/%m/%Y %H:%M:%S")

            return consulta


        def __obterHoraAtual(self):
            fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
            agora = datetime.now(fuso_horario)
            hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
            dia = agora.strftime('%Y-%m-%d')
            return hora_str

        def __obterDiaAtual(self):
            fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
            agora = datetime.now(fuso_horario)
            hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
            dia = agora.strftime('%Y-%m-%d')
            return dia

        def __dashboard_informacoes_faturamento_csw(self):
            """Método que obtém do CSW o dashboard de faturamento do período atual."""

            # 1. Obter os tipos de nota (sugestão: renomear para df_tipo_nota para clareza)
            df_tipo_nota = self.obterTipoNotasConsiderado().copy()
            df_tipo_nota = df_tipo_nota[df_tipo_nota['consideraTotalizador']=='true']

            # 2. Limpar a coluna 'tipoNota' (pegar apenas o código antes do '-')
            # O .strip() remove espaços em branco indesejados
            df_tipo_nota['tipoNota'] = df_tipo_nota['tipoNota'].astype(str).str.split('-').str[0].str.strip()

            # 3. Criar a cláusula para a consulta (ex: "1, 2, 3")
            # O .unique() evita enviar códigos duplicados para a consulta SQL
            clausula = ", ".join(df_tipo_nota['tipoNota'].unique())

            # 4. Instanciar a classe de Pedidos e realizar a consulta

            if self.codEmpresa == 'Todas':
                pedidos_csw1 = Pedidos_CSW.Pedidos_CSW(
                    '1', '', '', '', '', '', self.dataInicio, self.dataFim
                )
                pedidos_csw4 = Pedidos_CSW.Pedidos_CSW(
                    '4', '', '', '', '', '', self.dataInicio, self.dataFim
                )
                df_faturamento1 = pedidos_csw1.faturamento_csw_periodo(clausula)
                df_faturamento4 = pedidos_csw4.faturamento_csw_periodo(clausula)
                df_faturamento = pd.concat([df_faturamento1, df_faturamento4])


            else:

                pedidos_csw = Pedidos_CSW.Pedidos_CSW(
                    self.codEmpresa, '', '', '', '', '', self.dataInicio, self.dataFim
                )
                df_faturamento = pedidos_csw.faturamento_csw_periodo(clausula)

            # 5. Garantir que as chaves de junção sejam do mesmo tipo e estejam limpas
            df_faturamento['tipoNota'] = df_faturamento['tipoNota'].astype(str).str.strip()
            df_tipo_nota['tipoNota'] = df_tipo_nota['tipoNota'].astype(str).str.strip()

            # 6. Realizar o merge (junção) dos DataFrames
            consulta_final = pd.merge(df_faturamento, df_tipo_nota, on='tipoNota')

            return consulta_final
        def __obter_backup(self):
            '''Metodo que obtem os backups via arquivo csv'''

            tipoNota = self.obterTipoNotasConsiderado()
            tipoNota = tipoNota[tipoNota['consideraTotalizador']=='true']

            if self.codEmpresa == 'Todas':
                caminhoAbsoluto = configApp.localProjeto
                nome1 = f'{caminhoAbsoluto}/dados/FaturamentoAcumulado_' + '1'+ '.csv'
                nome4 = f'{caminhoAbsoluto}/dados/FaturamentoAcumulado_' + '4' + '.csv'

                consulta1 = pd.read_csv(nome1)
                consulta4 = pd.read_csv(nome4)

                consulta = pd.concat([consulta1,consulta4])


            else:

                caminhoAbsoluto = configApp.localProjeto
                nome = f'{caminhoAbsoluto}/dados/FaturamentoAcumulado_' + self.codEmpresa + '.csv'
                consulta = pd.read_csv(nome)



            consulta['tipoNota'] = consulta['tiponota'].astype(str)
            tipoNota['tipoNota'] = tipoNota['tipoNota'].astype(str).str.split('-').str[0].str.strip()

            consulta = pd.merge(consulta, tipoNota, on='tipoNota')
            print(f'backup{self.codAno}')
            consulta = consulta[consulta['dataEmissao'].str[:4] == str(self.codAno)]
            print(consulta)



            return consulta


        def __get_retorna(self):
            '''Metodo que busca os pedidos no retorna '''

            if self.codEmpresa == 'Todas':
                pedidosCsw1 = Pedidos_CSW.Pedidos_CSW('1','','','','','',self.dataInicio,self.dataFim).retorna_csw_empresa()
                pedidosCsw4 = Pedidos_CSW.Pedidos_CSW('4','','','','','',self.dataInicio,self.dataFim).retorna_csw_empresa()

                consulta = pd.concat([pedidosCsw1, pedidosCsw4])

            else:
                pedidosCsw = Pedidos_CSW.Pedidos_CSW(self.codEmpresa,'','','','','',self.dataInicio,self.dataFim)
                consulta = pedidosCsw.retorna_csw_empresa()

            return consulta

        def dashboard_view(self):
            """Método responsável pela Apresentação do Dashboard com valores formatados."""
            # 1. Tratamento de Datas
            data_hora_str = self.__obterHoraAtual()
            data_atual = datetime.strptime(data_hora_str, "%Y-%m-%d %H:%M:%S")
            self.dataInicio = data_atual.replace(day=1).strftime('%Y-%m-%d')
            self.dataFim = data_atual.strftime('%Y-%m-%d')

            # Função auxiliar para formatar moeda (R$)
            def formatar_real(valor):
                if pd.isna(valor) or valor == '-':
                    return "R$ 0,00"
                return f"R$ {valor:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')

            # 2. Processamento do "Retorna"
            df_retorna_csw = self.__get_retorna()
            df_retorna_csw["codPedido"] = df_retorna_csw["codPedido"].astype(str) + '-' + df_retorna_csw[
                "codSequencia"].astype(str)

            retorna_sb = df_retorna_csw[df_retorna_csw['codigo'] != 39]['vlrSugestao'].sum()
            retorna_mplus = df_retorna_csw[df_retorna_csw['codigo'] == 39]['vlrSugestao'].sum()

            # 3. Processamento de Faturamento
            meses_nomes = [
                '01-Janeiro', '02-Fevereiro', '03-Março', '04-Abril', '05-Maio', '06-Junho',
                '07-Julho', '08-Agosto', '09-Setembro', '10-Outubro', '11-Novembro', '12-Dezembro'
            ]


            df_backup = self.__obter_backup()


            caminhoAbsoluto = configApp.localProjeto
            nome_arquivo = f'FaturamentoDia_empresa_{self.codEmpresa}.csv'
            caminho_csv = os.path.join(caminhoAbsoluto, 'dados', nome_arquivo)
            # Flag para controlar se precisa recalcular
            precisa_atualizar = True

            # 1. Verifica se o arquivo existe e se é válido
            if os.path.exists(caminho_csv):
                try:
                    timestamp = os.path.getmtime(caminho_csv)
                    data_arquivo = datetime.fromtimestamp(timestamp)
                    diferenca = datetime.now() - data_arquivo

                    if diferenca < timedelta(minutes=1):
                        precisa_atualizar = False
                        print(f"Cache válido ({diferenca} atrás). Carregando arquivo...")
                    else:
                        print(f"Cache expirado ({diferenca} atrás).")

                except OSError:
                    # Se der erro ao ler a data do arquivo (arquivo corrompido ou bloqueado), força atualização
                    precisa_atualizar = True

            # 2. Bloco de Atualização (Executa apenas se necessário)
            if precisa_atualizar:
                print("Iniciando atualização dos dados...")

                try:
                    # Gera o DataFrame (Parte pesada)
                    df_mes_atual = self.__dashboard_informacoes_faturamento_csw()
                    df_mes_atual['dataHora'] = self.__obterDiaAtual()

                    # --- TÉCNICA DE ESCRITA ATÔMICA ---
                    # Salva num arquivo temporário primeiro para não corromper o original
                    # se alguém tentar ler enquanto você escreve.
                    fd, caminho_temp = tempfile.mkstemp(suffix=".csv", dir=os.path.dirname(caminho_csv))
                    os.close(fd)  # Fecha o descritor de arquivo de baixo nível

                    # Salva no temporário
                    df_mes_atual.to_csv(caminho_temp, index=False)

                    # Substitui o arquivo oficial pelo temporário de forma atômica (instantânea)
                    # Isso evita que o Pandas leia um arquivo "pela metade"
                    if os.path.exists(caminho_csv):
                        os.remove(caminho_csv)  # Necessário no Windows antes de renomear
                    os.rename(caminho_temp, caminho_csv)

                    print("Arquivo atualizado com sucesso.")

                except Exception as e:
                    print(f"Erro crítico ao atualizar cache: {e}")
                    # Se falhar a atualização, tenta carregar o antigo se existir para não quebrar a tela
                    if not os.path.exists(caminho_csv):
                        raise e  # Se não tem nem o antigo, estoura o erro

            # 3. Leitura Final
            try:
                # Agora é seguro ler
                df_mes_atual = pd.read_csv(caminho_csv)
            except Exception as e:
                print(f"Erro ao ler CSV, tentando regenerar: {e}")
                # Fallback de emergência: recalcula na memória sem salvar se o arquivo estiver inacessível
                df_mes_atual = self.__dashboard_informacoes_faturamento_csw()
                df_mes_atual['dataHora'] = self.__obterDiaAtual()

            df_mes_atual['dataHora'] = self.__obterDiaAtual()



            # Cálculo do faturamento do dia atual
            df_mes_atual['dataEmissao_dt'] = pd.to_datetime(df_mes_atual['dataEmissao'])
            faturado_dia = df_mes_atual[df_mes_atual['dataEmissao_dt'].dt.strftime('%Y-%m-%d') == self.dataFim][
                'faturado'].astype(float).sum()

            # 4. Consolidação Mensal
            consulta = pd.concat([df_backup, df_mes_atual], ignore_index=True)
            consulta = consulta[consulta['dataEmissao'].str[:4] == str(self.codAno)]

            consulta['dataEmissao'] = pd.to_datetime(consulta['dataEmissao'])
            consulta['mes'] = consulta['dataEmissao'].dt.month.apply(lambda x: meses_nomes[x - 1])

            # Agrupamento numérico
            df_agrupado = consulta.groupby("mes")["faturado"].sum().reset_index()
            df_final = pd.merge(pd.DataFrame({'mes': meses_nomes}), df_agrupado, on='mes', how='left').fillna(0)

            # Cálculo do Total Geral (ainda como número)
            total_geral = df_final['faturado'].sum()
            df_final['Fat.Acumulado'] = df_final['faturado'].cumsum()
            # --- NOVIDADE: Formatação da coluna faturado para a visualização ---
            # 1. Calcular os totais das colunas numéricas
            total_faturamento = df_final['faturado'].sum()
            metas = self.get_metas_cadastradas_ano_empresa()
            df_final = pd.merge(metas, df_final, on='mes',how='left')


            total_meta = df_final['meta'].str.replace('R$', '', regex=False).str.replace(' ', '').\
                str.replace('.', '').str.replace(',', '.').astype(float).sum()

            df_final['meta acum.'] =df_final['meta'].str.replace('R$', '', regex=False).str.replace(' ', '').\
                str.replace('.', '').str.replace(',', '.').astype(float).cumsum()


            df_final['mes'] = df_final['mes'].str.split('-').str[1]

            # 2. Criar um DataFrame de uma linha para o Total
            # As colunas de 'Acumulado' no total geralmente refletem o valor final cheio
            df_total = pd.DataFrame({
                'mes': ['✈TOTAL'],
                'faturado': [total_faturamento],
                'meta': formatar_real(total_meta),
                'Fat.Acumulado': [total_faturamento],
                'meta acum.': [total_meta]
            })

            # 3. Concatenar o total ao final do df_final
            df_final = pd.concat([df_final, df_total], ignore_index=True)



            df_final['faturado'] = df_final['faturado'].apply(formatar_real)
            df_final['Fat.Acumulado'] = df_final['Fat.Acumulado'].apply(formatar_real)
            df_final['meta acum.'] = df_final['meta acum.'].apply(formatar_real)

            df_final.rename(
                columns={'mes': 'Mês','faturado':"Faturado"},
                inplace=True)


            # 5. Montagem do Resultado
            data_dashboard = {
                '1- Ano:': str(self.codAno),
                '2- Empresa:': str(self.codEmpresa),
                '3- No Retorna': formatar_real(retorna_sb),
                '3.1- Retorna Mplus': formatar_real(retorna_mplus),
                '4- No Dia': formatar_real(faturado_dia),
                '5- TOTAL': formatar_real(total_geral),
                '6- Atualizado as': data_hora_str,
                '7- Detalhamento por Mes': df_final.to_dict(orient='records')
            }

            return pd.DataFrame([data_dashboard])
        def obterTipoNotasConsiderado(self):
            '''Metodo utilizado para carregar os tipo de notas sem pedidos '''

            query = """
            select
                "tipoNota", "consideraTotalizador"
            from
                "PCP"."DashbordTV"."confNota"
            """

            conn = ConexaoPostgre.conexaoEngine()

            consulta = pd.read_sql(query,conn)

            return consulta



        def configuracao_tipo_notas_empresa(self, consideraTotalizador = ''):

            '''Metodo que configura os tipos de notas por empresa '''


            delete = """
            delete from "PCP"."DashbordTV"."confNota"
            where "empresa" = %s
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(delete, (self.codEmpresa,))
                    conn.commit()



            # CORREÇÃO 1: Usar zip() para iterar duas listas simultaneamente
            for nota, consideraTotaliza in zip(self.codTipoNota, consideraTotalizador):

                verifica = self.get_tipo_notas_empresa_pornota(nota)

                if verifica.empty:

                    insert = """
                    insert into 
                        "PCP"."DashbordTV"."confNota"
                        (
                                        "tipoNota",
                                        "consideraTotalizador",
                                        "empresa"
                        ) values
                        ( %s, %s, %s )
                    """

                    with ConexaoPostgre.conexaoInsercao() as conn:
                        with conn.cursor() as curr:

                            curr.execute(insert,(nota, consideraTotaliza, self.codEmpresa))
                            conn.commit()

                else:
                    update = """
                    update 
                        "PCP"."DashbordTV"."confNota"
                    set 
                        "consideraTotalizador" = %
                    where
                        "tipoNota" = %s
                        and "empresa" = %s
                    """

                    with ConexaoPostgre.conexaoInsercao() as conn:
                        with conn.cursor() as curr:
                            curr.execute(update, (consideraTotaliza, nota, self.codEmpresa))
                            conn.commit()

            return pd.DataFrame([{'Mensagem':'Dados Gravados com sucesso ', 'status':True}])


        def get_tipo_notas_empresa(self):

            select = """
            select
                "tipoNota",
                "consideraTotalizador"
            from 
                "PCP"."DashbordTV"."confNota" aut 
            where
                empresa = %s
            """

            conn = ConexaoPostgre.conexaoEngine()

            consulta = pd.read_sql(select,conn,params=(self.codEmpresa,))
            return consulta



        def get_tipo_notas_empresa_pornota(self, nota):

            select = """
            select
                "tipoNota",
                "consideraTotalizador"
            from 
                "PCP"."DashbordTV"."confNota" aut 
            where
                empresa = %s and "tipoNota" = %s
            """

            conn = ConexaoPostgre.conexaoEngine()

            consulta = pd.read_sql(select,conn,params=(self.codEmpresa,nota))

            return consulta











