import pandas as pd
from src.models import Plano, Lote_Csw, Produtos

from src.connection import ConexaoPostgre


class Plano_Lote():
    '''Classe utilizada para interacao de Plano x lotes de producao '''
    def __init__(self, codEmpresa = '1', codPlano = None):

        self.codEmpresa = codEmpresa
        self.codPlano = codPlano

    def consultaVinculoLotes_Plano(self):
        '''Metodo que consulta a Lotes vinculado ao plano '''

        sql = """
            select 
                * 
            from 
                pcp."Plano" p 
            inner join 
                pcp."LoteporPlano"  lp 
                on lp.plano  = p.codigo 
            where p.codigo = %s
        """


        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        return consulta


    def vincularLotesAoPlano(self, arrayCodLoteCsw):
        '''Metodo que vincula lotes ao plano: Um plano pode ter varios Lotes Vinculados'''

        plano = Plano.Plano(self.codPlano)

        # Validando se o Plano ja existe
        validador = plano.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': f'O Plano {self.codPlano} NAO existe'}])
        else:

            # Deletando caso ja exista vinculo do lote no planto
            deleteVinculo = """Delete from pcp."LoteporPlano" where "lote" = %s AND plano = %s """

            # Inserindo o lote ao Plano
            insert = """insert into pcp."LoteporPlano" ("empresa", "plano","lote", "nomelote") values (%s, %s, %s, %s  )"""


            delete = """Delete from pcp.lote_itens where "codLote" = %s """



            conn = ConexaoPostgre.conexaoInsercao()
            cur = conn.cursor()

            for lote in arrayCodLoteCsw:
                self.codLote = lote
                nomelote = Lote_Csw.Lote_Csw(self.codLote).consultarLoteEspecificoCsw()
                cur.execute(deleteVinculo, (lote, self.codPlano,))
                conn.commit()
                cur.execute(insert, (self.codEmpresa, self.codPlano, lote, nomelote,))
                conn.commit()
                cur.execute(delete, (lote,))
                conn.commit()

            cur.close()
            conn.close()

            '''Metodo que insere no banco de dados os itens do lote '''
            self.explodindoAsReferenciasLote(arrayCodLoteCsw)

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes adicionados ao Plano com sucesso !'}])


    def explodindoAsReferenciasLote(self, arrayCodLoteCsw):
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)
        print(novo)

        lote_csw = Lote_Csw.Lote_Csw(novo, self.codEmpresa)
        lotes = lote_csw.getLoteSeqTamanhoCsw()

        # Implantando no banco de dados do Pcp
        ConexaoPostgre.Funcao_InserirOFF(lotes, lotes['codLote'].size, 'lote_itens', 'append')
        Produtos.Produtos().recarregar_novosSkus_noBanco()


        self.carregar_roteirosEngsLote(arrayCodLoteCsw)

        return lotes

    def carregar_roteirosEngsLote(self, arrayCodLoteCsw):
        '''Metodo que carrega no Postgres os roteiros da engenharia de acordo com o Lote '''

        # 1 - extraindo os lotes do arrayCodLoteCsw
        nomes_com_aspas = [f"'{nome}'" for nome in arrayCodLoteCsw]
        novo = ", ".join(nomes_com_aspas)

        # 2 Carregando os objetos dependentes
        lote_Csw = Lote_Csw.Lote_Csw(novo, self.codEmpresa)
        produtos = Produtos.Produtos()

        # 3 carregando as engeharias vinculadas ao lote

        EngRoteiro = lote_Csw.roteiroEng_CSW_peloLote()

        # 4 Verificando as engenharias que ja existe:
        produtosPCP = produtos.consultaEngComRoteirosCadastrados()

        # 5 descobrindo o que precisa ser atualizado

        EngRoteiro = pd.merge(EngRoteiro, produtosPCP, on='codEngenharia', how='left')
        EngRoteiro.fillna('-', inplace=True)
        EngRoteiro = EngRoteiro[EngRoteiro['situacao'] == '-'].reset_index()
        EngRoteiro = EngRoteiro.drop(columns=['situacao', 'index'])

        # 6 - executando a atualizacao
        if EngRoteiro['codEngenharia'].size > 0:
            # Implantando no banco de dados do Pcp
            ConexaoPostgre.Funcao_InserirOFF(EngRoteiro, EngRoteiro['codEngenharia'].size, 'Eng_Roteiro', 'append')
        else:
            print('segue o baile')


    def desvincularLotesAoPlano(self,arrayCodLoteCsw):

        plano = Plano.Plano(self.codPlano)

        # Validando se o Plano ja existe
        validador = plano.consultarPlano()
        validador = validador[validador['codigo'] == self.codPlano].reset_index()

        if validador.empty:

            return pd.DataFrame([{'Status': False, 'Mensagem': f'O Plano {self.codPlano} NAO existe'}])
        else:
            for lote in arrayCodLoteCsw:
                self.codLote = lote

                # Passo 1: Excluir o lote do plano vinculado
                deletarLote = """DELETE FROM pcp."LoteporPlano" WHERE lote = %s and plano = %s """
                conn = ConexaoPostgre.conexaoInsercao()
                cur = conn.cursor()
                cur.execute(deletarLote, (self.codLote, self.codPlano))
                conn.commit()

                # Passo 2: Verifica se o lote existe em outros planos
                conn2 = ConexaoPostgre.conexaoEngine()
                sql = """Select lote from pcp."LoteporPlano" WHERE lote = %s """
                verifca = pd.read_sql(sql, conn2, params=(self.codLote,))

                if verifca.empty:

                    deletarLoteIntens = """Delete from pcp.lote_itens where "codLote" = %s  """
                    cur.execute(deletarLoteIntens, (self.codLote,))
                    conn.commit()

                else:
                    print('sem lote para exlcuir dos lotes engenharias')
                cur.close()
                conn.close()

            return pd.DataFrame([{'Status': True, 'Mensagem': 'Lotes Desvinculados do Plano com sucesso !'}])








