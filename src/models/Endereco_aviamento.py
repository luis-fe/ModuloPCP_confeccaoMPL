from src.connection import ConexaoPostgre
import pandas as pd

class Endereco_aviamento():

    def __init__(self, endereco : str = '', rua : str = '', posicao : str = '', quadra :str = '', codItem : str = '',
                 dataHora : str = '' , qtd : int = 0, qtdConferida : int = 0, numeroOP : str = '', matricula = ''):

        self.endereco = endereco
        self.rua = rua
        self.posicao = posicao
        self.quadra = quadra
        self.codItem = codItem
        self.dataHora = dataHora
        self.qtd = qtd
        self.qtdConferida = qtdConferida
        self.numeroOP = numeroOP
        self.matricula = matricula



    def get_enderecos(self):

        consulta = """
        select * from "PCP".pcp."EnderecoReq" ur 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta


    def consulta_endereco_individual(self):

        consulta = """
        select * from "PCP".pcp."EnderecoReq" ur 
        where ur."endereco" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn, params=(self.endereco,))

        return consulta




    def insert_endereco(self):

        insert = """
        insert into "PCP".pcp."EnderecoReq" ( endereco , rua, quadra, posicao) values ( %s, %s, %s, %s )
        """
        self.endereco = f'{self.rua}-{self.quadra}-{self.posicao}'

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:



                curr.execute(insert, (self.endereco, self.rua, self.quadra, self.posicao))
                conn.commit()

    def reposicao_item_endereco(self, enderecoCorrigido, sequencia, usuario, matricula):

        insert = """
        insert into pcp."EnderecoReqItem" ( "endereco", "codItem", "qtd", "codItem_seq", "dataHora", "usuario", "matricula" )
        values ( %s, %s, %s, %s, %s, %s, %s)
        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:



                curr.execute(insert, (enderecoCorrigido, self.codItem, self.qtd, sequencia, self.dataHora, usuario, matricula))
                conn.commit()


    def get_ultima_sequencia_item(self):
        '''Metodo que busca a ultima sequencia de um kit item armazenado'''


        consulta= """
        select sequencia from pcp."Item_kit_seq"
        where "codMaterial" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(consulta,conn,params=(self.codItem,))

        return consulta


    def atualiza_inserir__sequenciaitem(self, sequencia):

        verifica = self.get_ultima_sequencia_item()

        if verifica.empty:

            inserir = """insert into pcp."Item_kit_seq" (sequencia , "codMaterial")
            values ( %s, %s )
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(inserir, (sequencia, self.codItem))
                    conn.commit()

        else:
            update = """update pcp."Item_kit_seq" set sequencia = %s where 
            "codMaterial" = %s
            """

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(update, (sequencia, self.codItem))
                    conn.commit()



    def produtividade_reposicao(self, dataInicio, dataFim):
        '''Metodo que busca na base de dados a produtividade'''

        consulta = """
        select
            usuario,
            count("codItem") as "qtd.Kit Reposto",
            count(distinct(endereco)) as "qtd Enderecos"
        from
            "PCP".pcp."EnderecoReqItem" eri
        where 
            "dataHora"::Date >= %s
            and "dataHora"::Date <= %s
            group by usuario 
            order by count("codItem") desc 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(dataInicio, dataFim))

        return consulta


    def get_item_qtd_op_CONFERENCIA(self):
        '''Metodo que busca se um item ja foi conferido'''


        consulta = """
        select * from pcp."AviamentosDisponiveis"
        where "numeroOP" = %s and "codMaterialEdt" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(self.numeroOP, self.codItem, ))

        return consulta


    def get_ops_paraConferir(self):
        '''Metodo que busca se um item ja foi conferido'''


        consulta = """
        select distinct "numeroOP", "codProduto", "FaseAtual" , "prioridade", "separador", "descricao" from pcp."AviamentosDisponiveis"
        where "seqRoteiro" not in ('408', '409')
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn)

        return consulta


    def get_carregar_itens_conferir(self):
        '''Metodo que busca se um item ja foi conferido'''


        consulta = """
        select  
            distinct "numeroOP", "codProduto", "FaseAtual" , "prioridade", "separador", "qtdeRequisitada", "codMaterialEdt",
            "nomeMaterial", "descricao" , "statusConferido"
        from 
            pcp."AviamentosDisponiveis"
        where 
            "numeroOP" = %s and "seqRoteiro" not in ('408', '409','426')
            AND  ("desconsideraConf" <> 'SIM' or "desconsideraConf" is null)
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta,conn,params=(self.numeroOP,))

        return consulta


    def validar_conferencia_item_etiquetado(self):
        '''Metdo publico que avalia se um item foi etiquetado '''

        verificar  = self.get_item_qtd_op_CONFERENCIA()

        if verificar.empty:
            return pd.DataFrame([{'Mensagem':'Item ainda nao conferido', 'status':False}])

        else:

            return pd.DataFrame([{'Mensagem':'Item ainda já conferido !', 'status':True}])


    def update_conferencia_item_op(self):
        '''Metodo publico que insere um intem no banco '''

        update = '''update  pcp."AviamentosDisponiveis"
            set "statusConferido" = 'Conferido'
            where
             "numeroOP" = %s and "codMaterialEdt" = %s
        '''

        verificar = self.get_item_qtd_op_CONFERENCIA()

        print(f'verificar numeroOP{self.numeroOP} and {self.codItem}:\n{verificar}')

        if not verificar.empty:

            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:

                    curr.execute(update,(self.numeroOP, self.codItem,))
                    conn.commit()


    def delete_item_conferecia_op(self):

        delete = """
        delete from pcp."AviamentoConfOP"
        where numeroOP = %s and "codMaterial" = %s and "qtd" = %s
        """

        verificar = self.get_item_qtd_op_CONFERENCIA()

        if not verificar.empty:
            with ConexaoPostgre.conexaoInsercao() as conn:
                with conn.cursor() as curr:
                    curr.execute(delete, (self.numeroOP, self.codItem, self.qtd))
                    conn.commit()


    def get_consulta_AviamentosDisponiveis(self):
        '''Metodo que busca a lista de AviamentosDisponiveis '''

        consulta = """
        select * from pcp."AviamentosDisponiveis" ad 
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(consulta,conn)

        return consulta

    def get_chaves_consulta_AviamentosDisponiveis(self):
        '''Metodo que busca a lista de AviamentosDisponiveis '''

        consulta = """
        select  
        distinct
        	"numeroOP",
	        "codMaterialEdt", 'ok' situacao
        from 
            pcp."AviamentosDisponiveis" ad 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(consulta, conn)

        return consulta


    def update_desconsidera_item_aviamento(self):

        update = """
        update pcp."AviamentosDisponiveis" set "desconsideraConf" = 'SIM' where "codMaterialEdt" = %s
        """

        with ConexaoPostgre.conexaoInsercao() as conn :
            with conn.cursor() as curr:

                curr.execute(update, (self.codItem,))
                conn.commit()


    def update_desconsidera_item_aviamento2(self):

        update = """
        update pcp."AviamentosDisponiveis" set "desconsideraConf" = 'SIM' where "codMaterialEdt" in
        (
                select
        DISTINCT
            ad."codMaterialEdt"        
            from
            "PCP".pcp."AviamentosDisponiveis" ad
        where
            "desconsideraConf" = 'SIM'
            )
        """

        with ConexaoPostgre.conexaoInsercao() as conn :
            with conn.cursor() as curr:

                curr.execute(update)
                conn.commit()


    def exluir_desconsidera_item_aviamento(self):

        update = """
        update pcp."AviamentosDisponiveis" set "desconsideraConf" = '-' where "codMaterialEdt" = %s
        """

        with ConexaoPostgre.conexaoInsercao() as conn :
            with conn.cursor() as curr:

                curr.execute(update, (self.codItem,))
                conn.commit()
    def buscar_nomeMaterial(self):

        select = """select distinct "nomeMaterial" from pcp."AviamentosDisponiveis"
        where "codMaterialEdt" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select, conn, params =(self.codItem,))

        return consulta

    def obter_itens_configuradados(self):

        select = """
        select
        DISTINCT
            ad."codMaterialEdt",
            "nomeMaterial"
        from
            "PCP".pcp."AviamentosDisponiveis" ad
        where
            "desconsideraConf" = 'SIM'
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(select,conn)

        return consulta


    def inserir_finalizacao_confencia(self):
        '''Metodo que insere quem finalisou uma determinada conferencia '''

        insert = '''
        insert into pcp."AviamentosConfFinalizacao" ("matricula" , "dataHora", "numeroOP") values ( %s, %s, %s)
        '''

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:

                curr.execute(insert,(self.matricula, self.dataHora, self.numeroOP ))
                conn.commit()


    def get_ops_conferidas(self):
        '''Metodo que obtem todas as Ops conferidas'''

        get = """
        select distinct "numeroOP", 'conferida' as status from pcp."AviamentosConfFinalizacao"
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(get,conn)

        return consulta


    def get_conferencia_periodo(self, dataInicio: str, dataFinal :str ):
        '''Metodo que busca o historico de conferencia finalizada em um determinado periodo'''

        get = """
        select 
            ur."nomeUsuario", 
            count("numeroOP") as "Ops Coferidas" 
        from 
            pcp."AviamentosConfFinalizacao" a
        inner join 
            pcp."usuarioReq" ur on ur."codMatricula" = a.matricula
        where 
            "dataHora"::Date >= %s and "dataHora"::Date <= %s 
        group by 
            ur."nomeUsuario"
        order by 
            count("numeroOP") desc
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(get, conn, params=(dataInicio, dataFinal))

        return consulta












