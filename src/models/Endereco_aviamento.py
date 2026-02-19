from src.connection import ConexaoPostgre
import pandas as pd

class Endereco_aviamento():

    def __init__(self, endereco : str = '', rua : str = '', posicao : str = '', quadra :str = ''):

        self.endereco = endereco
        self.rua = rua
        self.posicao = posicao
        self.quadra = quadra


    def get_enderecos(self):

        consulta = """
        select * from "PCP".pcp."EnderecoReq" ur 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta


    def insert_endereco(self):

        insert = """
        insert into "PCP".pcp."EnderecoReq" ( endereco , rua, quadra, posicao) values ( %s, %s, %s, %s )
        """
        self.endereco = f'{self.rua}-{self.quadra}-{self.posicao}'

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:



                curr.exectute(insert, self.endereco, self.rua, self.quadra, self.posicao)
                conn.commit()


