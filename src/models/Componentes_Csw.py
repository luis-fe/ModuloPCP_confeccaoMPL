import pandas as pd
import fastparquet as fp
from src.connection import ConexaoERP
from src.models import ServicoAutomacao


class Componentes_CSW():

    def __init__(self, codEmpresa = '1', intervalo_automacao = 100, ultima_atualizacao= ''):

        self.codEmpresa = codEmpresa
        self.intervalo_automacao = intervalo_automacao # Atrubuto para Controlar o intervalo de automacao em Segundos
        self.ultima_atualizacao = ultima_atualizacao




    def inserirComponentesVariaveis(self):

        sql = """
        SELECT 
                            v.codProduto as codEngenharia, 
                            cv.codSortimento, 
                            cv.seqTamanho as codSeqTamanho,  
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            cv.quantidade  
                        from 
                            tcp.ComponentesVariaveis v 
                        join 
                            tcp.CompVarSorGraTam cv 
                            on cv.codEmpresa = v.codEmpresa 
                            and cv.codProduto = v.codProduto 
                            and cv.sequencia = v.codSequencia 
                        WHERE 
                            v.codEmpresa = 1
                            and v.codClassifComponente <> 12
                        UNION 
                        SELECT 
                            v.codProduto as codEngenharia,  
                            l.codSortimento ,
                            l.codSeqTamanho  as codSeqTamanho, 
                            v.CodComponente,
                            (SELECT i.nome FROM cgi.Item i WHERE  i.codigo = v.CodComponente) as descricaoComponente,
                            (SELECT i.unidadeMedida FROM cgi.Item i WHERE i.codigo = v.CodComponente) as unid,
                            v.quantidade  
                        from 
                            tcp.ComponentesPadroes  v 
                        join 
                            cgi.Item2 l
                            on l.Empresa  = v.codEmpresa
                            and v.codEmpresa = 1
                            and l.codCor  > 0
                            and '0'||l.codItemPai ||'-0' = v.codProduto
                        where
                            l.Empresa  = 1
        """
        servicoAutomacao = ServicoAutomacao.ServicoAutomacao('3','insercao dos compontentes do csw no postgre')

        self.ultima_atualizacao = servicoAutomacao.obtentendo_intervalo_atualizacao_servico()
        print(f'ultima atualizacao {self.ultima_atualizacao}')
        if self.ultima_atualizacao > self.intervalo_automacao:

            with ConexaoERP.ConexaoInternoMPL() as conn:
                with conn.cursor() as cursor_csw:
                    # Executa a primeira consulta e armazena os resultados
                    cursor_csw.execute(sql)
                    colunas = [desc[0] for desc in cursor_csw.description]
                    rows = cursor_csw.fetchall()
                    compVar = pd.DataFrame(rows, columns=colunas)
                    del rows, colunas

            fp.write('./dados/compVar.parquet', compVar)
            dataHora = servicoAutomacao.obterHoraAtual()
            servicoAutomacao.inserindo_automacao(dataHora)