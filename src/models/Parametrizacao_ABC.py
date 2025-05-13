import pandas as pd

from src.connection import ConexaoPostgre


class Parametrizacao_ABC():
    '''Classe que gerencia as configuracoes das parametrizacoes ABC '''

    def __init__(self, codEmpresa = "1", parametroABC = ''):

        self.codEmpresa = codEmpresa
        self.parametroABC = parametroABC


    def consultaParametrizacaoABC(self):
        '''Metodo utilizado para consultar a parametrizacao do ABC'''

        sql = """
            select 
                "nomeABC"
            from 
                pcp."parametroABC"
            order by 
                "nomeABC" asc
        """
        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta


    def inserirParametroABC(self):
        '''Metodo utilizado para cadastrar um novo paramentro ABC'''

        inserir = """
        insert into pcp."parametroABC" ("nomeABC") values (%s)
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(inserir,(self.parametroABC,))
                conn.commit()

        return pd.DataFrame([{'status':True,'Mensagem':'Novo parametroABC inserido com sucesso'}])