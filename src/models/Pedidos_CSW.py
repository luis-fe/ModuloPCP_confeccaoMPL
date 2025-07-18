import gc

import pandas as pd
from src.connection import ConexaoERP


class Pedidos_CSW():
    '''Classe utilizado para interagir com os Pedidos do Csw '''

    def __init__(self, codEmpresa = '1', codTipoNota = '', filtroDataEmissaoIni = '',
                 filtroDataEmissaoFim = '', dataIniVendas = '', dataFinalVendas = '', dataInicioFat = '', dataFmFat = '',arrayTipoNota = ''):
        '''Construtor da classe '''

        self.codEmpresa = codEmpresa # codEmpresa
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
            ped.SugestaoPed c WHERE c.codEmpresa = """+str(self.codEmpresa)

        with ConexaoERP.ConexaoInternoMPL() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consultasqlCsw)
                colunas = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                consulta = pd.DataFrame(rows, columns=colunas)
            del rows
            return consulta


    def obtendoEntregas_Enviados(self):

        if self.codEmpresa == 4 or self.codEmpresa == '4' :
            consultasqlCsw = """
                select  
                    top 300000 codPedido, 
                    count(codNumNota) as entregas_enviadas, 
                    max(dataFaturamento) as ultimo_fat 
                from 
                    fat.NotaFiscal  
                where 
                    codEmpresa = 4 
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
        else:
            consultasqlCsw = """
                select  
                    top 300000 codPedido, 
                    count(codNumNota) as entregas_enviadas, 
                    max(dataFaturamento) as ultimo_fat 
                from 
                    fat.NotaFiscal  
                where 
                    codEmpresa = 1 
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

        if self.codEmpresa == 4 or self.codEmpresa == '4':

            consultasqlCsw = """
            select 
                top 100000 CAST(codPedido as varchar) as codPedido, 
                numeroEntrega as entregas_Solicitadas 
            from 
                asgo_ped.Entregas 
            where 
                codEmpresa = 4 
            order by 
                codPedido desc
            """

        else:
            consultasqlCsw = """
            select 
                top 100000 CAST(codPedido as varchar) as codPedido, 
                numeroEntrega as entregas_Solicitadas 
            from 
                asgo_ped.Entregas 
            where 
                codEmpresa = 1  
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




