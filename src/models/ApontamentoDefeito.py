'''Model do apontamento de defeito de 2a Qualidade - tabela pcp."ApntamentoDefeito"'''
import pandas as pd

from src.connection import ConexaoPostgre


class ApontamentoDefeito():
    '''
    Classe responsavel pelo CRUD do apontamento de defeito de 2a Qualidade.

    Tabela: pcp."ApntamentoDefeito"
        "dataHora"        timestamp   -> momento em que o apontamento foi gravado
        "dataApontamento" varchar(10) -> data informada na API ('YYYY-MM-DD' ou '-')
        "referencia"      varchar
        "cor"             varchar
        "tam"             varchar
        "op"              varchar
        "codTag"          varchar
        "usuario"         varchar
        "motivoDefeito"   varchar
        "detalhamento"    text
        "caminhoImg"      text        -> caminho absoluto da imagem gravada em /dados

    CHAVE DO REGISTRO:
        A tabela nao possui chave primaria (o layout definido tem apenas as colunas
        acima). O "caminhoImg" funciona como chave natural: cada apontamento grava
        um arquivo proprio (<codTag>_<motivoDefeito>.jpeg, com sufixo _2, _3...
        quando a mesma chave + motivo se repete), portanto atualizacao e exclusao
        sao feitas por ele.

    OBS: "dataApontamento" é varchar para tolerar o default '-' usado pelas APIs do
    projeto. O service normaliza a data para 'YYYY-MM-DD', formato em que a
    comparacao textual (>= / <=) coincide com a ordem cronologica.
    '''

    # Colunas da tabela, na ordem do layout - usada no insert e no select
    COLUNAS = [
        'dataHora', 'dataApontamento', 'referencia', 'cor', 'tam',
        'op', 'codTag', 'usuario', 'motivoDefeito', 'detalhamento', 'caminhoImg'
    ]

    # Colunas que a API deixa o usuario alterar em um apontamento ja gravado
    # ("caminhoImg" fica fora porque identifica a linha; "dataHora" é o momento
    # da gravacao e nao se altera)
    COLUNAS_EDITAVEIS = [
        'dataApontamento', 'referencia', 'cor', 'tam',
        'op', 'codTag', 'usuario', 'motivoDefeito', 'detalhamento'
    ]

    def __init__(self, dataHora=None, dataApontamento='-', referencia='-', cor='-', tam='-',
                 op='-', codTag='-', usuario='-', motivoDefeito='-', detalhamento='-',
                 caminhoImg='-'):
        '''
        :param dataHora: datetime do momento do apontamento (gerado pelo service)
        :param dataApontamento: data informada na API ('YYYY-MM-DD' ou '-')
        :param referencia: referencia do produto
        :param cor: cor do produto
        :param tam: tamanho do produto
        :param op: ordem de producao
        :param codTag: codigo da tag (etiqueta da peca)
        :param usuario: usuario que fez o apontamento
        :param motivoDefeito: motivo do defeito apontado
        :param detalhamento: descricao livre do defeito
        :param caminhoImg: caminho absoluto da imagem gravada
        '''
        self.dataHora = dataHora
        self.dataApontamento = dataApontamento
        self.referencia = referencia
        self.cor = cor
        self.tam = tam
        self.op = op
        self.codTag = codTag
        self.usuario = usuario
        self.motivoDefeito = motivoDefeito
        self.detalhamento = detalhamento
        self.caminhoImg = caminhoImg

    def criar_tabela(self):
        '''
        Metodo publico que cria a tabela e os indices de consulta caso ainda nao
        existam. É idempotente (create ... if not exists) e nao altera uma tabela
        pre-existente.
        '''
        ddl = '''
            create table if not exists pcp."ApntamentoDefeito" (
                "dataHora" timestamp,
                "dataApontamento" varchar(10),
                "referencia" varchar(60),
                "cor" varchar(60),
                "tam" varchar(30),
                "op" varchar(40),
                "codTag" varchar(60),
                "usuario" varchar(120),
                "motivoDefeito" varchar(250),
                "detalhamento" text,
                "caminhoImg" text
            );

            create index if not exists "ApntamentoDefeito_dataApontamento_idx"
                on pcp."ApntamentoDefeito" ( "dataApontamento" );

            create index if not exists "ApntamentoDefeito_op_idx"
                on pcp."ApntamentoDefeito" ( "op" );

            create index if not exists "ApntamentoDefeito_codTag_idx"
                on pcp."ApntamentoDefeito" ( "codTag" );
        '''

        conn = ConexaoPostgre.conexaoInsercao()

        try:
            with conn.cursor() as curr:
                curr.execute(ddl)

            conn.commit()
        finally:
            conn.close()

    def inserir_apontamento(self):
        '''
        Metodo publico que insere o apontamento de defeito.

        :return:
            quantidade de linhas inseridas (1 quando gravou)
        '''
        insert = '''
            insert into pcp."ApntamentoDefeito"
                ( "dataHora", "dataApontamento", "referencia", "cor", "tam",
                  "op", "codTag", "usuario", "motivoDefeito", "detalhamento", "caminhoImg" )
            values
                ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )
        '''

        parametros = (
            self.dataHora, self.dataApontamento, self.referencia, self.cor, self.tam,
            self.op, self.codTag, self.usuario, self.motivoDefeito, self.detalhamento,
            self.caminhoImg
        )

        conn = ConexaoPostgre.conexaoInsercao()

        try:
            with conn.cursor() as curr:
                curr.execute(insert, parametros)
                inseridos = curr.rowcount

            conn.commit()
        finally:
            conn.close()

        return inseridos

    def consultar_apontamentos(self, data_inicio='-', data_fim='-', textoAvancado=''):
        '''
        Metodo publico que consulta os apontamentos gravados. Os atributos da
        instancia funcionam como filtro: o que estiver em '-' (ou vazio) é ignorado.

        :param data_inicio: inicio do periodo de "dataApontamento" ('YYYY-MM-DD' ou '-')
        :param data_fim: fim do periodo de "dataApontamento" ('YYYY-MM-DD' ou '-')
        :param textoAvancado: busca livre em referencia, motivoDefeito e detalhamento
        :return:
            DataFrame (pandas) com as colunas da tabela, do apontamento mais
            recente para o mais antigo
        '''
        filtros = []
        parametros = []

        # Filtros de igualdade montados a partir dos atributos preenchidos
        for coluna, valor in (
            ('referencia', self.referencia),
            ('cor', self.cor),
            ('tam', self.tam),
            ('op', self.op),
            ('codTag', self.codTag),
            ('usuario', self.usuario),
            ('motivoDefeito', self.motivoDefeito),
            ('caminhoImg', self.caminhoImg),
            ('dataApontamento', self.dataApontamento),
        ):
            if self.__filtro_ativo(valor):
                filtros.append(f'and "{coluna}" = %s')
                parametros.append(str(valor))

        if self.__filtro_ativo(data_inicio):
            filtros.append('and "dataApontamento" >= %s')
            parametros.append(str(data_inicio))

        if self.__filtro_ativo(data_fim):
            filtros.append('and "dataApontamento" <= %s')
            parametros.append(str(data_fim))

        if self.__filtro_ativo(textoAvancado):
            filtros.append('''and ( "referencia" ilike %s
                                    or "motivoDefeito" ilike %s
                                    or "detalhamento" ilike %s )''')
            parametros.extend([f'%{textoAvancado}%'] * 3)

        consulta = f'''
            select
                "dataHora",
                "dataApontamento",
                "referencia",
                "cor",
                "tam",
                "op",
                "codTag",
                "usuario",
                "motivoDefeito",
                "detalhamento",
                "caminhoImg"
            from
                pcp."ApntamentoDefeito"
            where
                1 = 1
                {' '.join(filtros)}
            order by
                "dataHora" desc
        '''

        conn = ConexaoPostgre.conexaoEngine()

        return pd.read_sql(consulta, conn, params=tuple(parametros))

    def consultar_por_caminho(self):
        '''
        Metodo publico que consulta o apontamento pelo caminho da imagem
        (chave natural do registro).

        :return:
            DataFrame (pandas) com as colunas da tabela (vazio se nao existir)
        '''
        consulta = '''
            select
                "dataHora",
                "dataApontamento",
                "referencia",
                "cor",
                "tam",
                "op",
                "codTag",
                "usuario",
                "motivoDefeito",
                "detalhamento",
                "caminhoImg"
            from
                pcp."ApntamentoDefeito"
            where
                "caminhoImg" = %s
            order by
                "dataHora" desc
        '''

        conn = ConexaoPostgre.conexaoEngine()

        return pd.read_sql(consulta, conn, params=(self.caminhoImg,))

    def atualizar_apontamento(self, alteracoes):
        '''
        Metodo publico que atualiza as colunas informadas do apontamento
        identificado por "caminhoImg".

        :param alteracoes: dict { coluna: valor } restrito a COLUNAS_EDITAVEIS
        :return:
            quantidade de linhas atualizadas
        '''
        alteracoes = {
            coluna: valor
            for coluna, valor in (alteracoes or {}).items()
            if coluna in self.COLUNAS_EDITAVEIS
        }

        if not alteracoes:
            return 0

        set_colunas = ', '.join(f'"{coluna}" = %s' for coluna in alteracoes)

        update = f'''
            update
                pcp."ApntamentoDefeito"
            set
                {set_colunas}
            where
                "caminhoImg" = %s
        '''

        parametros = tuple(alteracoes.values()) + (self.caminhoImg,)

        conn = ConexaoPostgre.conexaoInsercao()

        try:
            with conn.cursor() as curr:
                curr.execute(update, parametros)
                atualizados = curr.rowcount

            conn.commit()
        finally:
            conn.close()

        return atualizados

    def excluir_apontamento(self):
        '''
        Metodo publico que exclui o apontamento identificado por "caminhoImg".
        A remocao do arquivo de imagem fica a cargo do service.

        :return:
            quantidade de linhas excluidas
        '''
        delete = '''
            delete from
                pcp."ApntamentoDefeito"
            where
                "caminhoImg" = %s
        '''

        conn = ConexaoPostgre.conexaoInsercao()

        try:
            with conn.cursor() as curr:
                curr.execute(delete, (self.caminhoImg,))
                excluidos = curr.rowcount

            conn.commit()
        finally:
            conn.close()

        return excluidos

    @staticmethod
    def __filtro_ativo(valor):
        '''Metodo privado que indica se o valor recebido deve virar filtro'''
        return valor not in (None, '', '-')
