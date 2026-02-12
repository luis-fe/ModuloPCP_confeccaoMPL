import gc

import pandas as pd
from src.connection import ConexaoERP



class OrdemProd_Csw():

    def __init__(self, codEmpresa = '1', dias_buscaCSW = 60, codFase = ''):

        self.codEmpresa = codEmpresa
        self.dias_buscaCSW = dias_buscaCSW
        self.codFase = codFase

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
                                e.descricao,
                                t.qtdOP, 
                                (select descricao from tcl.lote l where l.codempresa = {self.codEmpresa} and l.codlote = op.codlote) as descricaoLote  
                            FROM 
                                TCO.OrdemProd OP 
                            left JOIN 
                                tcp.PrioridadeOP p on p.codPrioridadeOP = op.codPrioridadeOP and op.codEmpresa = p.Empresa 
                            join 
                                tcp.engenharia e on e.codempresa = {self.codEmpresa} and e.codEngenharia = op.codProduto
                            left join 
                                (
                                SELECT numeroop, sum(qtdePecas1Qualidade) as qtdOP FROM tco.OrdemProdTamanhos  
                                where codempresa = {self.codEmpresa} group by numeroop
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

    def ops_baixadas_perido_csw(self, datainicial, datafinal):
        sql = """
                SELECT 
                    M.dataLcto , 
                    m.numDocto, 
                    m.qtdMovto, 
                    codNatureza1, 
                    m.codItem 
                FROM est.Movimento m
                WHERE 
                    codEmpresa = 1 and m.dataLcto >= '""" + datainicial + """'and m.dataLcto <= '""" + datafinal + """'
                    and operacao1 = '+' and numDocto like 'OP%'
                    AND codNatureza1 IN (5,7)
                    """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                opsBaixadas = pd.DataFrame(rows, columns=colunas)

            del rows

        return opsBaixadas

    def ops_baixadas_faccionista_costura(self, datainicial, datafinal):

        sql = f"""  
                    SELECT 
                        CONVERT(VARCHAR(6), R.codOP) AS OPpai, 
                        R.codFac,
                        (SELECT nome  FROM tcg.Faccionista  f WHERE f.empresa = 1 and f.codfaccionista = r.codfac) as nomeFaccicionista
                    FROM 
                        TCT.RemessaOPsDistribuicao R
                    INNER JOIN tco.OrdemProd op on
                        op.codempresa = r.empresa and op.numeroop = CONVERT(VARCHAR(10), R.codOP)
                    WHERE 
                        R.Empresa = 1 
                        and r.situac = 2 
                        and r.codOP in
                        (
							select 
								mf.numeroop 
							FROM 
								tco.MovimentacaoOPFase mf
							WHERE 
								mf.codempresa = 1 
								and mf.codfase = 429
								AND mf.databaixa >= DATEADD(DAY, -70, '{datainicial}')
					            AND mf.databaixa <= DATEADD(DAY, 70, '{datafinal}')
                        )  
                        and tiprem in (1 ,33) 
                        and r.codfase = 429
                    """



        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                opsBaixadas = pd.DataFrame(rows, columns=colunas)

            del rows

        return opsBaixadas


    def buscarProducao_erpCSW(self):
        '''Metodo privado que procura no ERP CSW a producao de acordo com o numero de dias informado para buscar, atributo :: self.dias_buscaCSW '''

        sql = f"""
            SELECT
                f.codEmpresa, 
                f.numeroop as numeroop, 
                f.codfase as codfase, 
                f.seqroteiro, 
                f.databaixa, 
                f.nomeFaccionista, 
                f.codFaccionista,
                f.horaMov, 
                f.totPecasOPBaixadas, 
                f.descOperMov, 
                (select op.codProduto  from tco.ordemprod op WHERE op.codempresa = f.codempresa and op.numeroop = f.numeroop) as codEngenharia,
                (select op.codTipoOP  from tco.ordemprod op WHERE op.codempresa = f.codempresa and op.numeroop = f.numeroop) as codtipoop,
				(select l.descricao from tcl.Lote l WHERE l.codEmpresa = f.codempresa and f.codLote = l.codLote) as descricaolote 
            FROM 
                tco.MovimentacaoOPFase f
            WHERE 
                f.codEmpresa in (1, 4) and f.databaixa >=  DATEADD(DAY, -{str(self.dias_buscaCSW)}, GETDATE())
            """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()


        # Monta a chave

        consulta['chave'] = consulta['codEmpresa'].astype(str) + '||' + consulta['numeroop'] + '||' + consulta['codfase'].astype(str)


        return consulta


    def ops_emAberto_movimentacao_fase(self):

        sql = f'''
                SELECT
                    m1.numeroop, 
                    dataBaixa 
                from
                    tco.MovimentacaoOPFase m1
                inner join 
                    tco.OrdemProd op
                    on op.codEmpresa = m1.codEmpresa 
                    and op.numeroOP = m1.numeroOP 
                WHERE
                    m1.codempresa = {str(self.codEmpresa)}
                    and m1.codfase = {str(self.codFase)}
                    and op.situacao = 3
            '''

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta



    def ordem_Prod_em_aberto(self):


        sql = f'''
        SELECT
            op.codPrioridadeOP  ,
            p.descricao as prioridade,
            op.numeroOP
        FROM
            tco.OrdemProd op
        inner join
            tcp.PrioridadeOP p 
            on p.Empresa = op.codEmpresa 
            and p.codPrioridadeOP = op.codPrioridadeOP 
        WHERE
            op.codEmpresa = {str(self.codEmpresa)}
            and op.situacao = 3
            '''



        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta


    def requisicaoes_ops_em_aberto(self):

        sql = f'''
	SELECT
            r.numero as requisicoes,
            r.numOPConfec as numeroOP,
            r.sitBaixa,
            R.seqRoteiro,
            case when r.sitBaixa = 1 then 'BAIXADA' ELSE 'EM ABERTO' END SITUACAO_REQUISICAO
        FROM
            tcq.Requisicao r
        inner join tco.OrdemProd op on
            op.codEmpresa = r.codEmpresa
            and op.numeroOP = r.numOPConfec
        WHERE
            r.codEmpresa = {str(self.codEmpresa)}
            and op.situacao = 3
            and r.seqRoteiro not in (408, 409)
        '''

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

        # Libera memória manualmente
        del rows
        gc.collect()

        return consulta




