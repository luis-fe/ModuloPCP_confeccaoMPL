import gc

import pandas as pd
from src.connection import ConexaoERP


class Pedidos_CSW():
    '''Classe utilizado para interagir com os Pedidos do Csw '''

    def __init__(self, codEmpresa = '1', codTipoNota = ''):
        '''Construtor da classe '''

        self.codEmpresa = codEmpresa # codEmpresa
        self.codTipoNota = codTipoNota

    def pedidosBloqueados(self):
        '''Metodo que pesquisa no Csw os pedidos bloqueados '''


        consultacsw = """
        SELECT 
            * 
        FROM 
            (
                SELECT top 300000 
                    bc.codPedido, 
                    'analise comercial' as situacaobloq  
                from 
                    ped.PedidoBloqComl  bc 
                WHERE 
                    codEmpresa = 1  
                    and bc.situacaoBloq = 1
                order by 
                    codPedido desc
                UNION 
                SELECT top 300000 
                    codPedido, 
                    'analise credito'as situacaobloq  
                FROM 
                    Cre.PedidoCreditoBloq 
                WHERE 
                    Empresa  = 1  
                    and situacao = 1
                order BY 
                    codPedido DESC
            ) as D"""

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultacsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            del rows
            return consulta

    def obtendoTipoNotaCsw(self):
        sql = """SELECT f.codigo , f.descricao  FROM fat.TipoDeNotaPadrao f"""

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                lotes = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return lotes

    def consultarTipoNotaEspecificoCsw(self):
        sql = """SELECT f.codigo , f.descricao  FROM fat.TipoDeNotaPadrao f where codigo = """ + str(self.codTipoNota)

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                nota = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return nota['descricao'][0]


