import pandas as pd

from src.connection import ConexaoPostgre



class Usuario_requisicao():

    def __init__(self, codMatricula: str = '', nomeUsuario : str ='', numeroOP : str = ''):

        self.codMatricula = codMatricula
        self.nomeUsuario = nomeUsuario
        self.numeroOP = numeroOP


    def inserir_usuario_op(self):


        insert = """
        insert into pcp."usuarioReqOP" (
            "codMatricula",
            "nomeUsuario", 
            "numeroOP" ) values (%s, %s, %s )
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.codMatricula, self.nomeUsuario, self.numeroOP))
                conn.commit()


    def update_usuario_op(self):


        insert = """
        update from pcp."usuarioReqOP"
            set "numeroOP" = %s
        where 
            "codMatricula" = %s 
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.numeroOP, self.codMatricula, ))
                conn.commit()


    def consulta_usuario_op(self):

        select = """
            select 
                "codMatricula",
                "nomeUsuario"
            from pcp."usuarioReqOP"
            where 
            "numeroOP" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select,conn, params=(self.numeroOP,))

        return consulta


    def inserir_atualizar_usuario_op(self):

        validador = self.consulta_usuario_op()

        if not validador.empty:

            self.update_usuario_op()
        else:
            self.inserir_usuario_op()

        return pd.DataFrame([{'Mensagem':'Operador inserido com sucesso','status':True}])


    def habilitar_usuario_separacao(self):

        validador = self.consulta_usuario_individual()

        if validador.empty:

            insert =  """ insert into pcp."usuarioReq"(
                        "codMatricula", 
                        "situacao",
                        "nomeUsuario" 
                ) values (%s,'Ativo', %s )
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(insert,(self.codMatricula, self.nomeUsuario,))
                    conn.commit()

        return pd.DataFrame([{'Mensagem':'Usuario Cadastrado com sucesso','status':True}])

    def get_usuarios_habilitados_req(self):

        select = """
        select 
            "codMatricula", 
            "nomeUsuario" 
        from pcp."usuarioReq"
        where 
            "situacao" = 'Ativo'
        ORDER BY "nomeUsuario"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn)

        return consulta

    def update_usuario_Ativo(self):

        insert = """
          update from pcp."usuarioReq"
              set "situacao" = %s
          where 
              "codMatricula" = %s 
          """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert, ('Ativo', self.codMatricula,))
                conn.commit()

    def consulta_usuario_individual(self):

        select = """
               select 
                   "codMatricula",
                   "nomeUsuario"
               from pcp."usuarioReq"
               where 
               "codMatricula" = %s
           """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn, params=(self.codMatricula,))

        return consulta







