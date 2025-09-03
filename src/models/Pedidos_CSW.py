import gc

import pandas as pd
from src.connection import ConexaoERP


class Pedidos_CSW():
    '''Classe utilizado para interagir com os Pedidos do Csw '''

    def __init__(self, codEmpresa = '1', codTipoNota = '', filtroDataEmissaoIni = '',
                 filtroDataEmissaoFim = '', dataIniVendas = '', dataFinalVendas = '', dataInicioFat = '', dataFmFat = '',arrayTipoNota = ''):
        '''Construtor da classe '''

        self.codEmpresa = str(codEmpresa) # codEmpresa
        self.codTipoNota = codTipoNota
        self.filtroDataEmissaoFim = filtroDataEmissaoFim
        self.filtroDataEmissaoIni = filtroDataEmissaoIni
        self.dataInicioFat = dataInicioFat
        self.dataFinalVendas = dataFinalVendas
        self.arrayTipoNota = arrayTipoNota
        self.dataIniVendas = dataIniVendas
        self.dataFmFat = dataFmFat

    def pedidosBloqueados(self):
        '''Metodo que pesquisa no Csw os pedidos bloqueados '''


        consultacsw = f"""
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
                    codEmpresa = {self.codEmpresa}   
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
                    Empresa  = {self.codEmpresa}  
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


    def capaPedidos(self):
        '''Metodo que busca a capa dos pedidos no periodo de vendas'''

        sql = f"""
                SELECT   
                    dataEmissao, 
                    convert(varchar(9), codPedido) as codPedido,
                    (select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli,
                    codTipoNota, 
                    dataPrevFat, 
                    convert(varchar(9),codCliente) as codCliente, 
                    codRepresentante, 
                    descricaoCondVenda, 
                    vlrPedido as vlrSaldo, 
                    qtdPecasFaturadas
                FROM 
                    Ped.Pedido p
                where 
                    codEmpresa = {str(self.codEmpresa)}
                    and  dataEmissao >= '{self.dataIniVendas}'
                    and dataEmissao <= '{self.dataFinalVendas}' 
                    and codTipoNota in ({self.arrayTipoNota})  """



        with ConexaoERP.ConexaoInternoMPL() as conn:
            consulta = pd.read_sql(sql, conn)
        return consulta



    def capaPedidosDataFaturamento(self):
        '''Metodo que busca a capa dos pedidos no periodo pela dataPrevFat -- dataprevicaoFaturamento'''

        if self.filtroDataEmissaoFim != '' and self.filtroDataEmissaoIni != '':
            sqlCswCapaPedidosDataPrev = f"""
                                            SELECT   
                                                dataEmissao, 
                                                convert(varchar(9), codPedido) as codPedido,
                                                (select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli,
                                                codTipoNota, 
                                                dataPrevFat, 
                                                convert(varchar(9),codCliente) as codCliente, 
                                                codRepresentante, 
                                                descricaoCondVenda, 
                                                vlrPedido as vlrSaldo, 
                                                qtdPecasFaturadas
                                            FROM 
                                                Ped.Pedido p
                                            where 
                                                codEmpresa = {str(self.codEmpresa)}
                                                and dataPrevFat >= '{self.dataInicioFat}'
                                                and dataPrevFat <= '{self.dataFmFat}'
                                                and dataEmissao >= '{self.filtroDataEmissaoIni}'
                                                and dataEmissao <= '{self.filtroDataEmissaoFim}'  
                                                and codTipoNota in ({self.arrayTipoNota})  
                                        """
        else:
            sqlCswCapaPedidosDataPrev = f"""
                                    SELECT   
                                        dataEmissao, 
                                        convert(varchar(9), codPedido) as codPedido,
                                        (select c.nome as nome_cli from fat.cliente c where c.codCliente = p.codCliente) as nome_cli,
                                        codTipoNota, 
                                        dataPrevFat, 
                                        convert(varchar(9),codCliente) as codCliente, 
                                        codRepresentante, 
                                        descricaoCondVenda, 
                                        vlrPedido as vlrSaldo, 
                                        qtdPecasFaturadas
                                    FROM 
                                        Ped.Pedido p
                                    where 
                                        codEmpresa = {str(self.codEmpresa)}
                                        and  dataPrevFat >= '{self.dataInicioFat}' 
                                        and dataPrevFat <= '{self.dataFmFat}' 
                                        and codTipoNota in ({self.arrayTipoNota})  """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(sqlCswCapaPedidosDataPrev)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)

            del rows
            return consulta


    def capaSugestao(self):
        consultasqlCsw = f"""
        SELECT 
            c.codPedido,
            situacaoSugestao as codSitSituacao ,
            case when (situacaoSugestao = 2 and dataHoraListagem>0) 
                then 'Sugerido(Em Conferencia)' 
                WHEN situacaoSugestao = 0 then 'Sugerido(Gerado)' WHEN situacaoSugestao = 2 then 'Sugerido(Em Conferencia)' 
                WHEN situacaoSugestao = 1 then 'Sugerido(Gerado)' else '' end StatusSugestao
        FROM 
            ped.SugestaoPed c WHERE c.codEmpresa = {str(self.codEmpresa)} """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta


    def obtendoEntregas_Enviados(self):


        consultasqlCsw = f"""
                select  
                    top 300000 codPedido, 
                    count(codNumNota) as entregas_enviadas, 
                    max(dataFaturamento) as ultimo_fat 
                from 
                    fat.NotaFiscal  
                where 
                    codEmpresa = {self.codEmpresa} 
                    and codRepresentante
                    not in ('200','800','300','600','700','511') 
                    and situacao = 2 
                    and codpedido> 0 
                    and dataFaturamento > '2020-01-01' 
                group by 
                    codPedido 
                order by 
                    codPedido desc
            """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta

    def obtendoEntregasSolicitadas(self):

        consultasqlCsw = f"""
            select 
                top 150000 CAST(codPedido as varchar) as codPedido, 
                numeroEntrega as entregas_Solicitadas 
            from 
                asgo_ped.Entregas 
            where 
                codEmpresa = {self.codEmpresa}  
            order by 
                codPedido desc
            """
        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta



    def faturamento_periodo_empresa(self):
        '''Metodo que busca o faturamento de um determinado periodo no CSW'''

        queryPorEmpresa = f"""
            SELECT 
                 n.codTipoDeNota as tiponota, 
                 n.dataEmissao, 
                 n.vlrTotal as faturado 
            FROM
                 Fat.NotaFiscal n 
            WHERE
                n.codPedido >= 0 
                and n.dataEmissao >= '{self.dataInicioFat}'
                and n.dataEmissao <= '{self.dataFmFat}'
                and situacao = 2
                and codEmpresa = {self.codEmpresa}
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queryPorEmpresa)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta


    def faturamento_nota_semPedidos_empresa(self):

        queryPorEmpresa = f"""
            SELECT 
                 n.codTipoDeNota as tiponota, 
                 n.dataEmissao, 
                 n.vlrTotal as faturado 
            FROM
                 Fat.NotaFiscal n 
            WHERE
                n.codTipoDeNota in (30, 180, 156, 51, 175, 81, 12, 47, 67, 149, 159, 1030, 2015, 1, 27, 102, 2, 9998) 
                and codPedido is null
                and n.dataEmissao >= '{self.dataInicioFat}'
                and n.dataEmissao <= '{self.dataFmFat}'
                and situacao = 2
                and codEmpresa = {self.codEmpresa}
        """


        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queryPorEmpresa)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta

    def faturamento_periodo_global(self):
        '''Metodo que busca o faturamento de um determinado periodo no CSW'''

        queryPorEmpresa = f"""
            SELECT 
                 n.codTipoDeNota as tiponota, 
                 n.dataEmissao, 
                 n.vlrTotal as faturado 
            FROM
                 Fat.NotaFiscal n 
            WHERE
                n.codPedido >= 0 
                and n.dataEmissao >= '{self.dataInicioFat}'
                and n.dataEmissao <= '{self.dataFmFat}'
                and situacao = 2
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queryPorEmpresa)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta

    def faturamento_nota_semPedidos_global(self):

        queryPorEmpresa = f"""
            SELECT 
                 n.codTipoDeNota as tiponota, 
                 n.dataEmissao, 
                 n.vlrTotal as faturado 
            FROM
                 Fat.NotaFiscal n 
            WHERE
                n.codTipoDeNota in (30, 180, 156, 51, 175, 81, 12, 47, 67, 149, 159, 1030, 2015, 1, 27, 102, 2, 9998) 
                and codPedido is null
                and n.dataEmissao >= '{self.dataInicioFat}'
                and n.dataEmissao <= '{self.dataFmFat}'
                and situacao = 2
        """

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(queryPorEmpresa)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta



    def retorna_csw_empresa(self):

        retornaCsw = f"""
                SELECT  
                    i.codPedido, 
                    e.vlrSugestao, 
                    sum(i.qtdePecasConf) as conf , 
                    sum(i.qtdeSugerida) as qtde,  
                    i.codSequencia,  
                    (SELECT codTipoNota  FROM ped.Pedido p WHERE p.codEmpresa = i.codEmpresa and p.codpedido = i.codPedido) as codigo 
                FROM 
                    ped.SugestaoPed e 
                inner join 
                    ped.SugestaoPedItem i 
                    on 
                        i.codEmpresa = e.codEmpresa 
                        and i.codPedido = e.codPedido 
                        and i.codsequencia = e.codsequencia 
                WHERE 
                    e.codEmpresa = {self.codEmpresa} 
                    and e.dataGeracao > '2023-01-01' 
                    and situacaoSugestao = 2
                group by 
                    i.codPedido, e.vlrSugestao,  i.codSequencia 
                """



        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(retornaCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta




