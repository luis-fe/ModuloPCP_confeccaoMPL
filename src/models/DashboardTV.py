import pandas as pd
from src.connection import ConexaoPostgre

class DashboardTV():
        ''''Classe responsavel pelo gerenciamento do Dashboard da TV '''

        def __init__(self, codEmpresa = '', codAno = '', meses = '', metas_valores = ''):

            self.codEmpresa = str(codEmpresa)
            self.codAno = str(codAno)
            self.meses = meses
            self.metas_valores = metas_valores


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









