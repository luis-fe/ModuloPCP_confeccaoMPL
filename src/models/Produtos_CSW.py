import gc
import pandas as pd
from src.conection import ConexaoERP

class Produtos_CSW():
    '''Classe utilizada para fazer buscas relativo aos Produtos cadastrados no ERP CSW'''

    def __init__(self, codProduto = None, ultimoItem = None):

        self.codProduto = codProduto
        self.ultimoItem = ultimoItem

    def get_itensFilhos_Novos_CSW(self):
        '''Metodo que busca no csw os novos itens filhos que ainda nao foram atualizados no banco Postgre desse projeto '''

        sqlCSWItens = """
                SELECT 
                    i.codigo , 
                    i.nome , 
                    i.unidadeMedida, 
                    i2.codItemPai, 
                    i2.codSortimento , 
                    i2.codSeqTamanho,
                    i2.codCor   
                FROM 
                    cgi.Item i
                JOIN 
                    Cgi.Item2 i2 on i2.coditem = i.codigo and i2.Empresa = 1
                WHERE 
                    i.unidadeMedida in ('PC','KIT') 
                    and (i2.codItemPai like '1%' or i2.codItemPai like '2%'or i2.codItemPai like '3%'or i2.codItemPai like '5%'or i2.codItemPai like '6%' )
                    and i2.codItemPai > 0 and i.codigo > """ + str(self.ultimoItem)

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlCSWItens)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            # Libera memória manualmente
        del rows
        gc.collect()


        return consulta
