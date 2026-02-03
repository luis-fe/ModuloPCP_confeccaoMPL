import pandas as pd
from src.connection.ConexaoPostgre import conexaoEngine
from src.connection import ConexaoPostgre
from src.models import OrdemProd_Csw, ServicoAutomacao

class OrdemProd ():

    def __init__(self, codEmpresa = '1', codSku = '', dataInicio = '', dataFinal ='', ulimosNdias=60, intervalo_automacao=200, limitePostgre=5000):

        self.codEmpresa = codEmpresa
        self.codSku = codSku
        self.dataInicio = dataInicio
        self.dataFinal = dataFinal
        self.ulimosNdias = ulimosNdias
        self.intervalo_automacao = intervalo_automacao,
        self.limitePostgre = limitePostgre


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



    def ops_baixas_faccionista_csw(self):
        '''Metodo que busca as ops baixadas no perido '''

        faccionista_ordemCsw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa).ops_baixadas_faccionista_costura(self.dataInicio, self.dataFinal)

        return faccionista_ordemCsw

    def realizado_fases_csw(self):
        '''Metodo que busca o realizado das fases no ERP CSW'''

        self.servicoAutomacao = ServicoAutomacao.ServicoAutomacao('05','Dados_CSW_RealizadoFase')
        self.ultima_atualizacao = self.servicoAutomacao.obtentendo_intervalo_atualizacao_servico()
        print(f'ultima_atualizacao: {self.ultima_atualizacao}')
        print(f'intervalo_automacao: {self.intervalo_automacao}')

        if self.ultima_atualizacao > self.intervalo_automacao:
            dataHora = self.servicoAutomacao.obterHoraAtual()
            self.servicoAutomacao.inserindo_automacao(dataHora)

            ordemCsw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa, self.ulimosNdias).buscarProducao_erpCSW()



            dadosAnteriores = self.__limpezaDadosRepetidos_ProducaoFasesPostgre()
            ordemCsw = pd.merge(ordemCsw, dadosAnteriores, on='chave', how='left')
            ordemCsw['status'].fillna('-', inplace=True)
            ordemCsw = ordemCsw[ordemCsw['status'] == '-'].reset_index()

            ordemCsw = ordemCsw.drop(columns=['status', 'index'])

            dataHora = self.servicoAutomacao.obterHoraAtual()
            self.servicoAutomacao.update_controle_automacao('etapa 1 - Busca sql', dataHora)

            if ordemCsw['numeroop'].size > 0:
                # Implantando no banco de dados do Pcp
                ConexaoPostgre.Funcao_InserirOFF_srvWMS(ordemCsw, ordemCsw['numeroop'].size, 'realizado_fase', 'append')

                sqlDelete = """
                    WITH duplicatas AS (
                        SELECT 
                            chave, 
                            ctid,
                            ROW_NUMBER() OVER (
                                PARTITION BY chave 
                                ORDER BY ctid ASC  -- ASC: menor ctid = mais antigo = será excluído
                            ) AS rn
                        FROM "pcp".realizado_fase
                    )
                    DELETE FROM "pcp".realizado_fase
                    WHERE ctid IN (
                        SELECT ctid 
                        FROM duplicatas 
                        WHERE rn > 1  -- mantém apenas o mais recente (ctid mais alto)
                    );
                        """

                conn1 = ConexaoPostgre.conexaoMatrizWMS()
                curr = conn1.cursor()
                curr.execute(sqlDelete, )
                conn1.commit()
                curr.close()
                conn1.close()

                self.servicoAutomacao.update_controle_automacao('Finalizado realizado fases', dataHora)


            else:
                print('segue o baile')


    def __limpezaDadosRepetidos_ProducaoFasesPostgre(self):
        '''Metodo que busca e limpa dados repetidos do banco Postgres'''

        sqlDelete = """
			WITH duplicatas AS 
				(
                SELECT chave, 
                        ctid,  -- Identificador físico da linha (evita deletar todas)
                        ROW_NUMBER() OVER (PARTITION BY chave ORDER BY ctid) AS rn
                FROM "pcp".realizado_fase
                    )
            DELETE FROM "pcp".realizado_fase
                WHERE ctid IN 
                    (
                        SELECT ctid FROM duplicatas WHERE rn > 1
                    );
        """

        conn1 = ConexaoPostgre.conexaoMatrizWMS()
        curr = conn1.cursor()
        curr.execute(sqlDelete, )
        conn1.commit()
        curr.close()
        conn1.close()

        sql2 = """
            DELETE FROM pcp.realizado_fase
            WHERE ctid IN (
                SELECT ctid
                FROM pcp.realizado_fase
                ORDER BY chave DESC
                LIMIT 30000
            );
        """

        conn1 = ConexaoPostgre.conexaoMatrizWMS()
        curr = conn1.cursor()
        curr.execute(sql2, )
        conn1.commit()
        curr.close()
        conn1.close()



        sql = """
        select distinct CHAVE, 'ok' as status from pcp.realizado_fase
        order by CHAVE desc limit %s
        """

        conn = ConexaoPostgre.conexaoMatrizWMS()
        consulta = pd.read_sql(sql, conn, params=(self.limitePostgre,))




        return consulta
