import gc

import pandas as pd
from src.connection import ConexaoERP



class OrdemProd_Csw():

    def __init__(self, codEmpresa = '1'):

        self.codEmpresa = codEmpresa

    def get_fasesCsw(self):
        '''Metodo que retorna as fases do ERP CSW'''

        sql = """
        SELECT
            convert(varchar(5),codFase) as codFaseAtual,
            nome as nomeFase
        FROM
            tcp.FasesProducao
        where
            codempresa = 1 
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            del rows
            return consulta



    def informacoesMonitor(self):
        sqlCsw = """
                Select 
                    f.codFase as codFaseAtual , 
                    f.nome  
                from 
                    tcp.FasesProducao f 
                WHERE 
                    f.codEmpresa = 1
                    """
        sqlCswPrioridade = f"""
                           SELECT 
                                op.numeroOP as numeroop, 
                                p.descricao as prioridade, 
                                op.dataPrevisaoTermino, 
                                e.descricao,t.qtdOP, 
                                (select descricao from tcl.lote l where l.codempresa = 1 and l.codlote = op.codlote) as descricaoLote  
                            FROM 
                                TCO.OrdemProd OP 
                            left JOIN 
                                tcp.PrioridadeOP p on p.codPrioridadeOP = op.codPrioridadeOP and op.codEmpresa = p.Empresa 
                            join 
                                tcp.engenharia e on e.codempresa = 1 and e.codEngenharia = op.codProduto
                            left join 
                                (
                                SELECT numeroop, sum(qtdePecas1Qualidade) as qtdOP FROM tco.OrdemProdTamanhos  
                                where codempresa = 1 group by numeroop
                                ) t on t.numeroop =op.numeroop
                            WHERE 
                                op.situacao = 3 and op.codEmpresa = {self.codEmpresa}
                           """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor_csw:
                # Executa a primeira consulta e armazena os resultados
                cursor_csw.execute(sqlCsw)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                get = pd.DataFrame(rows, columns=colunas)
                get['codFaseAtual'] = get['codFaseAtual'].astype(str)
                del rows

                cursor_csw.execute(sqlCswPrioridade)
                colunas = [desc[0] for desc in cursor_csw.description]
                rows = cursor_csw.fetchall()
                get2 = pd.DataFrame(rows, columns=colunas)
                del rows


                return  get, get2


