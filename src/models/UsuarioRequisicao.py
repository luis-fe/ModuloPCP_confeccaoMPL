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

        insert =  """ insert into pcp."usuarioReq"(
                    "codMatricula",
            "nomeUsuario", 
            ) values (%s, %s )
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.codMatricula, self.nomeUsuario,))
                conn.commit()

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






