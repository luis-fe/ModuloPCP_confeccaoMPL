import pandas as pd
from src.connection.ConexaoPostgre import conexaoEngine
from src.models import OrdemProd_Csw
class OrdemProd ():

    def __init__(self, codEmpresa = '1' , codSku = ''):

        self.codEmpresa = codEmpresa
        self.codSku = codSku


    def get_OrdemProdSku(self):
        '''Metodo que busca as ordem de producao de um determinado sku ao nivel de tamanho e cor '''


        sql = """
        select
            numeroop,
            "codFaseAtual",
            "total_pcs"
        from
            "PCP".pcp.ordemprod o
        where
            o.codreduzido = %s
        order by 
        "total_pcs"::int desc 
        """
        conn = conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codSku,))

        ordemProd_Csw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa)

        fasesCsw = ordemProd_Csw.get_fasesCsw()

        consulta = pd.merge(consulta, fasesCsw, on='codFaseAtual', how='left')

        return consulta