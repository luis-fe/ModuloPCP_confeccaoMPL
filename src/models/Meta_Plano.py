import pandas as pd

from src.models import Plano
from  src.connection import ConexaoPostgre


class Meta_Plano():
    '''Classe que interage com as Metas que tem vinculo com o Plano '''

    def __init__(self, codEmpresa = '1' , codPlano = None, marca = None, metaFinanceira = 0, metaPecas = 0):

        self.codEmpresa = codEmpresa
        self.codPlano = codPlano
        self.marca = marca
        self.metaFinanceira = metaFinanceira
        self.metaPecas = metaPecas


    def consultaMetaGeral(self):
        '''Método utilizado para consultar a Meta Geral.'''

        sql = """
        SELECT 
            codigo as "codPlano",
            p."descricaoPlano",
            "marca",
            "metaFinanceira",
            "metaPecas"
        FROM
            "pcp"."Metas" m
        right JOIN
            "pcp"."Plano" p ON p.codigo = m."codPlano" 
        WHERE
            "codigo" = %s
        """

        # Obtem a conexão com o banco
        conn = ConexaoPostgre.conexaoEngine()

        # Realiza a consulta no banco de dados
        consulta = pd.read_sql(sql, conn, params=(self.codPlano,))

        # Função para tratar e formatar a string "R$xxxxxxx"
        def __formatar_meta_financeira(valor):
            try:
                valor_limpo = float(valor.replace("R$", "").replace(",", "").strip())
                return f'R$ {valor_limpo:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
            except ValueError:
                return valor  # Retorna o valor original caso não seja convertível


        def __formatar_meta_pecas(valor):
            try:
                valor_limpo = int(valor)
                return f'{valor_limpo:,.0f}'.replace(",", "X").replace("X", ".")
            except ValueError:
                return valor  # Retorna o valor original caso não seja convertível

        def __formatar_meta_financeira_int(valor):
                # Remove o prefixo "R$", pontos e vírgulas, e converte para float
                valor_limpo = int(valor.replace("R$", "").replace(".", ""))
                return valor_limpo

        def __formatar_meta_financeira_float(valor):
                # Remove o prefixo "R$", pontos e vírgulas, e converte para float
                valor_limpo = float(valor.replace("R$", "").replace(".", "").replace(",", ".").strip())
                return valor_limpo

        consulta['metaFinanceira'].fillna('R$ 0,00',inplace=True)
        consulta['metaPecas'].fillna('0',inplace=True)

        consulta.fillna('-',inplace=True)

        # Aplica o tratamento à coluna 'metaFinanceira' (formatação monetária)
        consulta['metaFinanceira'] = consulta['metaFinanceira'].apply(__formatar_meta_financeira)
        consulta['metaPecas'] = consulta['metaPecas'].apply(__formatar_meta_pecas)


        totalFinanceiro = consulta['metaFinanceira'].apply(__formatar_meta_financeira_float).sum()
        totalPecas = consulta['metaPecas'].apply(__formatar_meta_financeira_int).sum()

        # Cria a linha de total
        total = pd.DataFrame([{
            'codPlano': self.codPlano,
            'descricaoPlano': consulta['descricaoPlano'].iloc[0],
            'marca': 'TOTAL',
            'metaFinanceira': f'R$ {totalFinanceiro:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),  # Soma os valores numéricos
            'metaPecas': f'{totalPecas:,.0f}'.replace(",", "X").replace("X", ".")
        }])

        # Concatena o total ao DataFrame original
        consulta = pd.concat([consulta, total], ignore_index=True)

        return consulta
    def inserirOuAtualizarMetasGeraisPlano(self):
        '''Metodo utilizado para inserir ou atualizar as metas gerais por Plano e Marca'''
        if self.metaFinanceira == None:
            return pd.DataFrame([{'status': False, "mensagem": 'Metas Financeira nao encontrada'}])

        if self.metaPecas == None:
            return pd.DataFrame([{'status': False, "mensagem": 'Metas em Pecas nao encontrada'}])

        verifica = self.consultaMetaGeralPlanoMarca()

        if verifica.empty:
            self.__cadastrarMetaGeral()# Cadastra as novas metas (financeira e quantitativa) a nivel de Marca
            self.__inserirMetaSemanalPlanoMarca()
        else:
            self.__updateMetaGeral()

        return pd.DataFrame([{'status':True, "mensagem":'Metas inseridas com sucesso'}])
    def consultarMetaCategoriaPlano(self):
        '''metodo que realiza a consulta da meta por categoria do plano e da Marca '''

        consulta1 = self.consultaMetaGeralPlanoMarca()

        if consulta1.empty:
            totalPecas = 0
            totalReais = 0
        else:
            consulta1['metaPecas'] = consulta1['metaPecas'].str.replace('.','')
            totalPecas = consulta1['metaPecas'][0]
            totalPecas = int(totalPecas)

            consulta1['metaFinanceira'] = consulta1['metaFinanceira'].str.replace('R\$', '', regex=True).str.strip()
            consulta1['metaFinanceira'] = consulta1['metaFinanceira'].str.replace('.','').str.replace(',','x').str.replace('x','.')
            totalReais = consulta1['metaFinanceira'][0]
            totalReais = float(totalReais)
        sql1 = """
        select
			m."nomeCategoria" ,
			m."metaPc" ,
			m."metaFinanceira" 
		from
			pcp."Meta_Categoria_Plano" m
		where 
			"codPlano" = %s 
			and "marca" = %s
        """

        sql2 ="""
        select
			distinct "nomeCategoria"
		from
			pcp."Categorias" c
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta1 = pd.read_sql(sql1,conn,params=(self.codPlano, self.marca))
        consulta1['metaPc'] = consulta1['metaPc'].apply(self.__formatar_padraoInteiro)
        consulta1['metaFinanceira'] = consulta1['metaFinanceira'].apply(self.__formatar_financeiro)

        consulta2 = pd.read_sql(sql2,conn)

        consulta = pd.merge(consulta2, consulta1, on=['nomeCategoria'],how='left')
        consulta.fillna('-', inplace=True)



        data = {
                '1- Plano - Marca:': f'({self.codPlano})-{self.marca}',
                '2- Total Pecas':f'{totalPecas:,.0f}'.replace(",", "X").replace("X", "."),
                '3 - Total R$':f'R$ {totalReais:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."),
                '4- DetalhamentoCategoria': consulta.to_dict(orient='records')
            }
        return pd.DataFrame([data])
    def consultaMetaGeralPlanoMarca(self):
        ''''Metodo para consultar as metas de um plano por Plano e Marca '''

        sql = """
        select 
            "codPlano",
            "marca",
            "metaFinanceira",
            "metaPecas"
        from
            "pcp"."Metas"
        where
            "codPlano" = %s and "marca" = %s
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn,params=(self.codPlano, self.marca))

        return consulta

    def __inserirMetaSemanalPlanoMarca(self):
        '''Metodo que verica quantas semanas de vendas possue o plano, e prepara os dados'''

        insert = """
        insert into 
            "pcp"."MetasSemanal" ("codPlano", "marca", semana, distribuicao ,"metaFinanceira", "metaPeca" )
        values 
            ( %s, %s, %s , %s ,%s, %s )
        """


        numeroSemanas = Plano.Plano(self.codPlano).obterNumeroSemanasVendas()

        self.distribuicao = '0'
        self.metaFinanceira = '0'
        self.metaPecas = '0'

        if numeroSemanas > 0:
            for i in range(numeroSemanas):
                self.semana = i +1

                with ConexaoPostgre.conexaoInsercao() as conn:
                    with conn.cursor() as curr:
                        curr.execute(insert,(self.codPlano, self.marca, self.semana, self.distribuicao, self.metaFinanceira, self.metaPecas))
                        conn.commit()
    def __cadastrarMetaGeral(self):
        '''metodo PRIVADO criado para cadastrar as metas gerais '''

        insert = """
        insert into 
            "pcp"."Metas"
        ("codPlano",
            "marca",
            "metaFinanceira",
            "metaPecas")
        values 
            (%s, %s, %s, %s)
        """

        self.metaFinanceira = self.__converterFloat(self.metaFinanceira)

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(insert, (self.codPlano, self.marca, self.metaFinanceira, self.metaPecas))

    def __updateMetaGeral(self):

        update = """
        update "pcp"."Metas"
        set
            "metaFinanceira" = %s,
            "metaPecas" = %s
        where
        "marca" = %s and "codPlano" = %s

        """

        with ConexaoPostgre.conexaoInsercao() as conn:
            with conn.cursor() as curr:
                curr.execute(update, (self.metaFinanceira, self.metaPecas, self.marca, self.codPlano))


    def __converterFloat(self, valor):
        """Metodo Privado que identifica o valor da variavel e retorna o valor em float """

        if isinstance(valor, float):

            return valor
        elif isinstance(valor, int):
            valor = float(valor)

            return valor

        else:
            # Remover "R$"
            if "R$" in valor:
                valor = valor.replace("R$", "").strip()
            valor = valor.replace(".", "").replace(",", ".")
            # Converter para float
            valor_float = float(valor)

            return valor_float

    def __formatar_padraoInteiro(self, valor):
        "metodo que converte valor para formato inteiro int"
        try:
            return f'{valor:,.0f}'.replace(",", "X").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível


    def __formatar_financeiro(self, valor):
        "metodo que converte valor para formato financeiro int"

        try:
            return f'R$ {valor:,.2f}'.replace(",", "X").replace(".", ",").replace("X", ".")
        except ValueError:
            return valor  # Retorna o valor original caso não seja convertível




