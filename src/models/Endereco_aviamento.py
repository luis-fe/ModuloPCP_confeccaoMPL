from src.connection import ConexaoPostgre
import pandas as pd

class Endereco_aviamento():

    def __init__(self, endereco : str = '', rua : str = '', posicao : str = '', quadra :str = '', codItem : str = '',
                 dataHora : str = '' , qtd : int = 0, qtdConferida : int = 0, numeroOP : str = ''):

        self.endereco = endereco
        self.rua = rua
        self.posicao = posicao
        self.quadra = quadra
        self.codItem = codItem
        self.dataHora = dataHora
        self.qtd = qtd
        self.qtdConferida = qtdConferida
        self.numeroOP = numeroOP



    def get_enderecos(self):

        consulta = """
        select * from "PCP".pcp."EnderecoReq" ur 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta


    def consulta_endereco_individual(self):

        consulta = """
        select * from "PCP".pcp."EnderecoReq" ur 
        where ur."endereco" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.endereco,))

        return consulta




    def insert_endereco(self):

        insert = """
        insert into "PCP".pcp."EnderecoReq" ( endereco , rua, quadra, posicao) values ( %s, %s, %s, %s )
        """
        self.endereco = f'{self.rua}-{self.quadra}-{self.posicao}'

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:



                curr.execute(insert, (self.endereco, self.rua, self.quadra, self.posicao))
                conn.commit()

    def reposicao_item_endereco(self):

        insert = """
        insert into pcp."EnderecoReqItem" ( "endereco", "codItem", "dataHora", "qtd" )
        values ( %s, %s, %s, %s)
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:



                curr.execute(insert, self.endereco, self.codItem, self.dataHora, self.qtd)
                conn.commit()


    def get_item_qtd_op_CONFERENCIA(self):
        '''Metodo que busca se um item ja foi conferido'''


        consulta = """
        select * from pcp."AviamentosDisponiveis"
        where "numeroOP" = %s and "codMaterialEdt" = %s and "statusConferido" <> 'Conferido'
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(self.numeroOP, self.codItem, self.qtdConferida))

        return consulta


    def get_ops_paraConferir(self):
        '''Metodo que busca se um item ja foi conferido'''


        consulta = """
        select distinct "numeroOP", "codProduto", "FaseAtual" , "prioridade", "separador" from pcp."AviamentosDisponiveis"
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn)

        return consulta


    def get_itens_paraConferir(self):
        '''Metodo que busca se um item ja foi conferido'''


        consulta = """
        select  distinct "numeroOP", "codProduto", "FaseAtual" , "prioridade", "separador", "qtdeRequisitada", "codMaterialEdt",
        "nomeMaterial" 
        from pcp."AviamentosDisponiveis"
        where "numeroOP" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(self.numeroOP,))

        return consulta


    def validar_conferencia_item_etiquetado(self):
        '''Metdo publico que avalia se um item foi etiquetado '''

        verificar  = self.get_item_qtd_op_CONFERENCIA()

        if verificar.empty:
            return pd.DataFrame([{'Mensagem':'Item ainda nao conferido', 'status':False}])

        else:

            return pd.DataFrame([{'Mensagem':'Item ainda já conferido !', 'status':True}])


    def update_conferencia_item_op(self):
        '''Metodo publico que insere um intem no banco '''

        update = '''update  pcp."AviamentoConfOP" (
            set "statusConferido" = 'Conferido'
             "numeroOP" = %s and "codMaterialEdt" = %s
        '''

        verificar = self.get_item_qtd_op_CONFERENCIA()

        if verificar.empty:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(update,(self.numeroOP, self.codItem,))
                    conn.commit()


    def delete_item_conferecia_op(self):

        delete = """
        delete from pcp."AviamentoConfOP"
        where numeroOP = %s and "codMaterial" = %s and "qtd" = %s
        """

        verificar = self.get_item_qtd_op_CONFERENCIA()

        if not verificar.empty:
            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(delete, (self.numeroOP, self.codItem, self.qtd))
                    conn.commit()


    def get_consulta_AviamentosDisponiveis(self):
        '''Metodo que busca a lista de AviamentosDisponiveis '''

        consulta = """
        select * from pcp."AviamentosDisponiveis" ad 
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(consulta,conn)

        return consulta

    def get_chaves_consulta_AviamentosDisponiveis(self):
        '''Metodo que busca a lista de AviamentosDisponiveis '''

        consulta = """
        select  
        	"numeroOP",
	        "codMaterialEdt", 'ok' situacao
        from 
            pcp."AviamentosDisponiveis" ad 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta





