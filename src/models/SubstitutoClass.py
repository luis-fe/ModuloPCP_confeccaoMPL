from src.connection import ConexaoPostgre
import pandas as pd
from src.models import Produtos_CSW

class Substituto():
    def __init__(self, codMateriaPrima = '' , codMateriaPrimaSubstituto = '', nomeCodMateriaPrima = None, nomeCodSubstituto = None):

        self.codMateriaPrima = codMateriaPrima
        self.codMateriaPrimaSubstituto = codMateriaPrimaSubstituto
        self.nomeCodMateriaPrima = nomeCodMateriaPrima
        self.nomeCodSubstituto = nomeCodSubstituto


        produto_Csw = Produtos_CSW.Produtos_CSW()

        self.pesquisarNomeMaterial = produto_Csw.pesquisarNomeMaterial(self.codMateriaPrima)

        self.nomeCodSubstituto = produto_Csw.pesquisarNomeMaterial(self.codMateriaPrima)

        self.nomeCodSubstituto = self.nomeCodSubstituto['nome'][0]
        self.nomeCodMateriaPrima = self.nomeCodMateriaPrima['nome'][0]

    def consultaSubstitutos(self):
        '''Metodo que consulta todos os substitutos '''

        sql = """
        select 
            "codMateriaPrima",
            "nomeCodMateriaPrima",
            "codMateriaPrimaSubstituto",
            "nomeCodSubstituto"
        from
            pcp."SubstituicaoMP"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def inserirSubstituto(self):
        '''Metodo que insere um substituto'''


        insert = """Insert into pcp."SubstituicaoMP" ("codMateriaPrima" , "nomeCodMateriaPrima" , "codMateriaPrimaSubstituto", "nomeCodSubstituto") 
        values ( %s, %s, %s, %s )"""

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.codMateriaPrima, self.nomeCodMateriaPrima, self.codMateriaPrimaSubstituto, self.nomeCodSubstituto))
                conn.commit()

    def updateSubstituto(self):
        '''Metodo que insere um substituto'''

        self.nomeCodSubstituto = self.pesquisarNomeMaterial(self.codMateriaPrimaSubstituto)
        self.nomeCodSubstituto = self.nomeCodSubstituto['nome'][0]
        update = """update  pcp."SubstituicaoMP" 
        set 
            "codMateriaPrimaSubstituto" = %s , "nomeCodSubstituto" =%s
        where 
            "codMateriaPrima" = %s 
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(update,(self.codMateriaPrimaSubstituto, self.nomeCodSubstituto, self.codMateriaPrima))
                conn.commit()





    def inserirOuAlterSubstitutoMP(self):
        '''Metodo que insere ou altera o substituto de uma materia prima '''

        #1 - Verifica se a materia prima ja possui substituto
        verifica = self.verificarMP()

        if verifica.empty:
            self.inserirSubstituto()
        else:
            self.updateSubstituto()

        return pd.DataFrame([{'status':True,'Mensagem':'Substituto inserido ou alterado com sucesso! '}])

    def verificarMP(self):
        '''Metodo que verifica se a Materia Prima ja possui substituto'''

        sql = """
        select 
            "codMateriaPrima",
            "nomeCodMateriaPrima",
            "codMateriaPrimaSubstituto",
            "nomeCodSubstituto"
        from
            pcp."SubstituicaoMP"
        where
            "codMateriaPrima" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn, params=(self.codMateriaPrima,))

        return consulta



