from src.connection import ConexaoPostgre
import pandas as pd

class Materia_prima_aviamento():


    def __init__(self, codEmpresa = '1', categoria = '', tam_padrao_kit : int = 1,
                 tam_padrao2_kit: int = 1,tam_padrao3_kit : int = 1, granel :int = 1, regra_distribuicao :str = ''
                 ):

        self.codEmpresa = codEmpresa
        self.categoria = categoria
        self.tam_padrao_kit = tam_padrao_kit
        self.tam_padrao2_kit  = tam_padrao2_kit
        self.tam_padrao3_kit = tam_padrao3_kit
        self.granel = granel
        self.regra_distribuicao = regra_distribuicao

    def get_configurar_categoria_material(self):

        sql = """
        select 
            categoria, 
            "tam_padrao_kit" , 
            "tam_padrao2_kit",
            "tam_padrao3_kit", 
            "granel", 
            "regraDistribuicao"
        from 
            pcp."AviamentosConfiguraco"
        """
        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql,conn)

        return consulta


    def inserir_configurar_categoria_material(self):

        insert = """
            insert into 
                pcp."AviamentosConfiguraco" 
                (categoria, "tam_padrao_kit" , "tam_padrao2_kit","tam_padrao3_kit", "granel", "regraDistribuicao" )
            values 
                ( %s, %s, %s , %s )
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.categoria, self.tam_padrao_kit, self.tam_padrao2_kit, self.tam_padrao3_kit, self.granel, self.regra_distribuicao))
                conn.commit()



    def configuracao_de_para_descricao(self):

        sql = """
        select 
            "descricao_contem" , 
            "categoria"
        from 
            pcp."Aviamento_de_para"   
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql, conn)

        return consulta


    def update_categoria_configuracao(self):

        update = """
        update pcp."Aviamento_de_para"
        set 
        """



