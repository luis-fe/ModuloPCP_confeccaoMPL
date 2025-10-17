import pandas as pd
from src.connection.ConexaoPostgre import conexaoEngine
from src.models import OrdemProd_Csw
class OrdemProd ():

    def __init__(self, codEmpresa = '1' , codSku = '', dataInicio = '', dataFinal =''):

        self.codEmpresa = codEmpresa
        self.codSku = codSku
        self.dataInicio = dataInicio
        self.dataFinal = dataFinal


    def get_OrdemProdSku(self):
        '''Metodo que busca as ordem de producao de um determinado sku ao nivel de tamanho e cor '''


        sql = f"""
        select
            numeroop,
            "codFaseAtual",
            "total_pcs"
        from
            pcp.ordemprod o
        where
            o.codreduzido = %s
            and "codEmpresa" = {self.codEmpresa}
        order by 
        "total_pcs"::int desc 
        """
        conn = conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codSku,))

        ordemProd_Csw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa)

        fasesCsw = ordemProd_Csw.get_fasesCsw()

        consulta = pd.merge(consulta, fasesCsw, on='codFaseAtual', how='left')

        return consulta


    def ops_baixas_csw(self):
        '''Metodo que busca as ops baixadas no perido '''

        ordemCsw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa).ops_baixadas_perido_csw(self.dataInicio, self.dataFinal)

        return ordemCsw