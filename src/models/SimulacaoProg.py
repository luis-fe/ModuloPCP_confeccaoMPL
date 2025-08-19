import pandas as pd
from src.connection import ConexaoPostgre

class SimulacaoProg():
    '''Classe utilizada para a simulacao da programacao '''

    def __init__(self, nomeSimulacao = None, classAbc = None, perc_abc = None, categoria = None, marca = None, empresa = '1'):

        self.nomeSimulacao = nomeSimulacao
        self.classAbc = classAbc
        self.perc_abc = perc_abc
        self.categoria = categoria
        self.marca = marca
        self.codEmpresa = empresa

    def __inserirSimulacao(self):
        '''metodo que faz a insersao de uma nova simulacao'''

        verifica = self.consultaSimulacao()

        if not verifica.empty:
            return pd.DataFrame([{'status':False, "mensagem": "já existe essa simulacao"}])
        else:
            inserir = '''
                        insert into pcp."Simulacao" ("nomeSimulacao") values ( %s ) 
                        '''
            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(inserir,(self.nomeSimulacao,))
                    conn.commit()

            return pd.DataFrame([{'status':True, "mensagem": "Simulacao inserida com sucesso"}])



    def consultaSimulacao(self):
        '''metodo que consulta uma simulacao em especifico'''

        select = """
        select "nomeSimulacao" from pcp."Simulacao" where "nomeSimulacao" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,))

        return consulta


    def inserirAbcSimulacao(self):
        '''metodo que inseri a simulacao nos niveis abc '''

        verfica = self.consultaSimulacaoAbc()

        if not verfica.empty:
            update = f"""
                update  
                    pcp."SimulacaoAbc" 
                set 
                    percentual = %s 
                where 
                    "nomeSimulacao" = %s 
                    and "class" = %s
                    and "codEmpresa" = '{self.codEmpresa}'
                       """
            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(update,(self.perc_abc,self.nomeSimulacao, self.classAbc))
                    conn.commit()

        else:

            insert = """
            insert into pcp."SimulacaoAbc" ("nomeSimulacao", "class", percentual, "codEmpresa") values ( %s, %s, %s, %s )
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert,(self.nomeSimulacao,self.classAbc, self.perc_abc, self.codEmpresa))
                    conn.commit()


            return pd.DataFrame([{'status':True, "mensagem": "Simulacao inserida com sucesso"}])


    def inserirCategoriaSimulacao(self):
        '''metodo que inseri a simulacao nos niveis abc '''

        verfica = self.consultaSimulacaoCategoria()

        if not verfica.empty:
            update = """
            update  pcp."SimulacaoCategoria" set percentual = %s where "nomeSimulacao" =%s and "categoria" = %s
            """
            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(update,(self.perc_abc,self.nomeSimulacao, self.categoria))
                    conn.commit()

        else:

            insert = """
            insert into pcp."SimulacaoCategoria" ("nomeSimulacao", "categoria", percentual) values ( %s, %s, %s )
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert,(self.nomeSimulacao,self.categoria, self.perc_abc))
                    conn.commit()


            return pd.DataFrame([{'status':True, "mensagem": "Simulacao inserida com sucesso"}])


    def inserirMarcaSimulacao(self):
        '''metodo que inseri a simulacao nos niveis abc '''

        verfica = self.consultaSimulacaoMarca()

        if not verfica.empty:
            update = """
            update  pcp."SimulacaoMarca" set percentual = %s where "nomeSimulacao" =%s and "marca" = %s
            """
            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(update,(self.perc_abc,self.nomeSimulacao, self.marca))
                    conn.commit()

        else:

            insert = """
            insert into pcp."SimulacaoMarca" ("nomeSimulacao", "marca", percentual) values ( %s, %s, %s )
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert,(self.nomeSimulacao,self.marca, self.perc_abc))
                    conn.commit()


            return pd.DataFrame([{'status':True, "mensagem": "Simulacao inserida com sucesso"}])




    def consultaSimulacaoAbc(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            * 
        from 
            pcp."SimulacaoAbc" s 
        Where 
            "nomeSimulacao" = %s 
            and class = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,self.classAbc,))

        return consulta

    def consultaSimulacaoAbc_s(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            class, percentual as "percentualABC" 
        from 
            pcp."SimulacaoAbc" s 
        Where 
            "nomeSimulacao" = %s 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,))

        return consulta


    def consultaSimulacaoCategoria(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            * 
        from 
            pcp."SimulacaoCategoria" s 
        Where 
            "nomeSimulacao" = %s 
            and categoria = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,self.categoria,))

        return consulta


    def consultaSimulacaoCategoria_s(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
	        categoria ,
	        percentual as "percentualCategoria"
        from
	        pcp."SimulacaoCategoria" s
	    where
            "nomeSimulacao" = %s 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,))

        return consulta

    def consultaSimulacaoMarca(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            * 
        from 
            pcp."SimulacaoMarca" s 
        Where 
            "nomeSimulacao" = %s 
            and marca = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,self.marca,))

        return consulta

    def consultaSimulacaoMarca_s(self):
        '''Metodo que consulta uma simulacao ABC por nome em especifico'''

        select = """
        select 
            marca, percentual as "percentualMarca"  
        from 
            pcp."SimulacaoMarca" s 
        Where 
            "nomeSimulacao" = %s 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn , params=(self.nomeSimulacao,))

        return consulta

    def get_Simulacoes(self):
        '''metodo que consulta uma simulacao em especifico'''

        select = """
        select "nomeSimulacao" from pcp."Simulacao"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn )

        return consulta

    def consultaDetalhadaSimulacao(self):
        '''metodo que consulta a simulacao em detalhes'''

        consultaParamentrosAbc = f"""
        select
            distinct "nomeABC" as class
        from
            pcp."Plano_ABC" pa
        where 
            pa."codEmpresa" = '{self.codEmpresa}'
        order by
            "nomeABC" asc 
        """

        consultaAbcSimulacao = f"""
        select 
            "class",
            percentual
        from 
            pcp."SimulacaoAbc" s 
        Where 
            "nomeSimulacao" = %s 
            and "codEmpresa" = '{self.codEmpresa}'
        """

        consultaCategoriaSimulacao = """
        select 
            "categoria",
            percentual
        from 
            pcp."SimulacaoCategoria" s 
        Where 
            "nomeSimulacao" = %s 
        """

        consultaCategoria= """
        select "nomeCategoria" as categoria from pcp."Categorias" c 
        where c."nomeCategoria" <> '-'
        order by "nomeCategoria" asc 
        """

        consultaMarcas= """
        select
            marca
        from
            pcp."Marcas" m
        """

        consultaMarcaSimulacao = """
        select 
            "marca",
            percentual
        from 
            pcp."SimulacaoMarca"
        Where 
            "nomeSimulacao" = %s 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consultaParamentrosAbc = pd.read_sql(consultaParamentrosAbc, conn)
        consultaAbcSimulacao = pd.read_sql(consultaAbcSimulacao,conn , params=(self.nomeSimulacao,))

        consultaSimulacao = pd.merge(consultaParamentrosAbc, consultaAbcSimulacao , on='class', how='left')
        consultaSimulacao['percentual'].fillna(0, inplace=True)

        consultaCategoriaSimulacao = pd.read_sql(consultaCategoriaSimulacao,conn , params=(self.nomeSimulacao,))
        consultaCategoria = pd.read_sql(consultaCategoria, conn)
        consultaCategoria = pd.merge(consultaCategoria,consultaCategoriaSimulacao,on='categoria', how='left' )
        consultaCategoria['percentual'].fillna(100, inplace=True)

        consultaMarcaSimulacao = pd.read_sql(consultaMarcaSimulacao,conn , params=(self.nomeSimulacao,))
        consultaMarcas = pd.read_sql(consultaMarcas, conn)
        consultaMarcas = pd.merge(consultaMarcas,consultaMarcaSimulacao,on='marca', how='left' )
        consultaMarcas['percentual'].fillna(100, inplace=True)



        consultaSimulacaoNome = self.consultaSimulacao()



        data = {
                '1- Simulacao': f'{consultaSimulacaoNome["nomeSimulacao"][0]}',
                '2- ABC':consultaSimulacao.to_dict(orient='records'),
                '3- Categoria': consultaCategoria.to_dict(orient='records'),
                '4- Marcas': consultaMarcas.to_dict(orient='records')
        }
        return pd.DataFrame([data])


    def inserirAtualizarSimulacao(self, arrayAbc , arrayMarca, arrayCategoria):
        '''Metedo utilizado para atualizar ou inserir a simulacao '''

        self.__inserirSimulacao()

        if arrayAbc != []:
            # 1 - transformacao do array abc em DataFrame
            AbcDataFrame = pd.DataFrame({
                'class': arrayAbc[0],
                'percentual': arrayAbc[1]
            })

            for _, row in AbcDataFrame.iterrows():  # O índice é descartado com '_'
                self.classAbc = row['class']  # Acessa diretamente o valor da coluna
                self.perc_abc = row['percentual']  # Acessa diretamente o valor da coluna
                self.inserirAbcSimulacao()

        if arrayCategoria != []:
            # 1 - transformacao do array abc em DataFrame
            CategoriaDataFrame = pd.DataFrame({
                'categoria': arrayCategoria[0],
                'percentual': arrayCategoria[1]
            })

            for _, row in CategoriaDataFrame.iterrows():  # O índice é descartado com '_'
                self.categoria = row['categoria']  # Acessa diretamente o valor da coluna
                self.perc_abc = row['percentual']  # Acessa diretamente o valor da coluna
                self.inserirCategoriaSimulacao()

        if arrayMarca != []:
            # 1 - transformacao do array abc em DataFrame
            MarcaDataFrame = pd.DataFrame({
                'marca': arrayMarca[0],
                'percentual': arrayMarca[1]
            })

            for _, row in MarcaDataFrame.iterrows():  # O índice é descartado com '_'
                self.marca = row['marca']  # Acessa diretamente o valor da coluna
                self.perc_abc = row['percentual']  # Acessa diretamente o valor da coluna
                self.inserirMarcaSimulacao()



        return pd.DataFrame([{'Mensagem':'Simulacao inserida ou alterada com sucesso','satus':True}])

    def excluirSimulacao(self):
        '''Metodo utilizado para excluir uma simulacao'''

        delete = """
        DELETE from pcp."Simulacao"
        where "nomeSimulacao" = %s
        """

        deleteMarca = """
        DELETE from 
            pcp."SimulacaoMarca"
        where 
            "nomeSimulacao" = %s
        """

        deleteCategoria= """
        DELETE from 
            pcp."SimulacaoCategoria"
        where 
            "nomeSimulacao" = %s
        """

        deleteAbc = """
         DELETE from 
             pcp."SimulacaoAbc"
         where 
             "nomeSimulacao" = %s
         """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(delete, (self.nomeSimulacao, ))
                conn.commit()

                curr.execute(deleteCategoria, (self.nomeSimulacao, ))
                conn.commit()

                curr.execute(deleteMarca, (self.nomeSimulacao, ))
                conn.commit()

                curr.execute(deleteAbc, (self.nomeSimulacao, ))
                conn.commit()

        return pd.DataFrame([{'status': True, "mensagem": "Simulacao deletada com sucesso"}])






