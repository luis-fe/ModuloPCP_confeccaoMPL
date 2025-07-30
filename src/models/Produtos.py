import pandas as pd
import fastparquet as fp
from src.configApp import configApp
from src.models import Produtos_CSW, Produtos_colorBook
from src.connection import ConexaoPostgre


class Produtos():
    '''Classe utilizada para o gerenciamento dos produtos (sku)  que compoe o PCP Confeccao'''

    def __init__(self, codEmpresa = '1', codSku = None, codNatureza = '5', codItemPai = '', indice = 0):

        self.codEmpresa = codEmpresa
        self.codSku = codSku
        self.codNatureza = codNatureza
        self.codItemPai = codItemPai
        self.indice = indice

    def __obter_UltimoSkuCadastrosNoPCP(self):
        '''Metodo que obtem os skus cadastrados no banco postgres da aplicacao '''


        sql = """
        select max(codigo::int) as maximo from pcp.itens_csw
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql, conn)
        ultimoSku = consulta['maximo'][0]

        return ultimoSku

    def recarregar_novosSkus_noBanco(self):
        '''Metodo utilizado para recarregar novos Sku's no banco de dados Postgres para utilizar na Plantaforma PCP '''

        ultimoSku = self.__obter_UltimoSkuCadastrosNoPCP()

        produtos_CSW = Produtos_CSW.Produtos_CSW(self.codEmpresa,ultimoSku)

        # 1 - Obter itens ainda nao cadastrados no postgre
        consulta_novosItens = produtos_CSW.get_itensFilhos_Novos_CSW()

        # 2 - Atribuir categoria
        consulta_novosItens['categoria'] = '-'
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('CAMISA', row['nome'], 'CAMISA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('POLO', row['nome'], 'POLO', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BATA', row['nome'], 'CAMISA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('TRICOT', row['nome'], 'TRICOT', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BONE', row['nome'], 'BONE', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('TSHIRT', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('REGATA', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BLUSAO', row['nome'], 'AGASALHOS', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BABY', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('JAQUETA', row['nome'], 'JAQUETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BONE', row['nome'], 'BONE', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('CINTO', row['nome'], 'CINTO', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('PORTA CAR', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('CUECA', row['nome'], 'CUECA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('MEIA', row['nome'], 'MEIA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('SUNGA', row['nome'], 'SUNGA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('SHORT', row['nome'], 'SHORT', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BERMUDA', row['nome'], 'BERMUDA M', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BRINDES', row['nome'], 'DISPLAY', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BRINDES', row['nome'], 'CARTAZ', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.__definir_Categoria('BRINDES', row['nome'], 'ADESIVO', row['categoria']), axis=1)


        try:
            # Implantando no banco de dados do Pcp na tabela itens_csw
            ConexaoPostgre.Funcao_InserirOFF(consulta_novosItens, consulta_novosItens['codigo'].size, 'itens_csw', 'append')
        except:
            print('segue o baile ')
        return consulta_novosItens



    def __definir_Categoria(self, contem, valorReferencia, valorNovo, categoria):
        if contem in valorReferencia:
            return valorNovo
        else:
            return categoria

    def consultaEngComRoteirosCadastrados(self):
        '''Metodo que consulta no postgres as engenharias com roteiros cadastrados '''

        sqlPCP = """select distinct "codEngenharia", 'ok' as situacao from pcp."Eng_Roteiro" """

        conn = ConexaoPostgre.conexaoEngine()
        sqlPCP = pd.read_sql(sqlPCP, conn)

        return sqlPCP



    def consultaItensReduzidos(self):
        '''Metodo utilizado para consultar os itens reduzidos x informacoes: codPai , codCor , nome, categoria'''

        sql = """
        	select
                codigo,
                nome,
                "codItemPai",
                "codCor",
                categoria,
                "codSeqTamanho",
                "codSortimento"
            from
                pcp.itens_csw ic
            where
                "codItemPai" not like '6%' 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn)
        #consulta['codigo'] = consulta['codigo'].astype(str).str.replace('.0','')
        return consulta


    def estoqueNat(self):
        '''Metodo que consulta o estoque por natureza e empresa '''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None,self.codNatureza)
        consulta = produto_Csw.estoqueNat()

        return consulta

    def emProducao(self):
        '''Metodo que consulta o estoque em processo a nivel sku '''


        sql = """
        select
            ic.codigo as "codReduzido",
            sum(total_pcs) as "emProcesso"
        from
            pcp.itens_csw ic
        inner join 
            pcp.ordemprod o 
            on substring(o."codProduto",2,8) = "codItemPai"
            and o."codSortimento" = ic."codSortimento"::varchar
            and o."seqTamanho" = "codSeqTamanho"::varchar
        where o."codProduto" not like  '6%'
        group by 
            ic.codigo 
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def get_tamanhos(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.get_tamanhos()

        return consulta

    def statusAFV(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.statusAFV()

        return consulta


    def estMateriaPrima(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.estMateriaPrima()

        return consulta

    def estMateriaPrima_nomes(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.sqlEstoqueMP_nomes()

        return consulta


    def req_Materiais_aberto(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.req_Materiais_aberto()

        return consulta

    def req_atendidoComprasParcial(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.req_atendidoComprasParcial()

        return consulta

    def pedidoComprasMP(self):
        '''Metodo que obtem os tamanhos cadastrados'''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.pedidoComprasMP()

        return consulta

    def pesquisarNomeMaterial(self):
        '''Metodo '''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.pesquisarNomeMaterial()

        return consulta

    def informacoesComponente(self):
        '''Metodo '''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None)
        consulta = produto_Csw.informacoesComponente()

        return consulta


    def codColceaoPai(self):
        '''Metodo que busca a colecao de um item '''

        produto_Csw = Produtos_CSW.Produtos_CSW(self.codEmpresa,None,None,None,self.codItemPai)
        consulta = produto_Csw.obterColecaoItemPai()

        return consulta['colecao'][0]

    def imagemColorBook(self):
        """Metodo que capitura a imagem de um produto do color book"""


        produto_colorBook = Produtos_colorBook.Produtos_colorBook(str(self.codItemPai), str(self.codColceaoPai()))
        imagem = produto_colorBook.obtendoImagemColorBook()

        return imagem

    def imagem_S_ColorBook(self):
        """Metodo que capitura a imagem de um produto do color book"""


        produto_colorBook = Produtos_colorBook.Produtos_colorBook(str(self.codItemPai), str(self.codColceaoPai()),self.indice)
        resposta = produto_colorBook.obtendoImagem_S_ColorBook()

        return resposta




    def carregandoComponentes(self):
        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        caminho_absoluto = configApp.localArquivoParquet
        # 1.2 - Carregar o arquivo Parquet
        parquet_file = fp.ParquetFile(f'{caminho_absoluto}/compVar.parquet')


        # Converter para DataFrame do Pandas
        df_loaded = parquet_file.to_pandas()

        return df_loaded

    def categoriasDisponiveis(self):
        '''Metodo que consulta as marcas disponiveis '''

        sql = """
        select * from pcp."Categorias"
        order by "nomeCategoria" asc
        """

        conn = ConexaoPostgre.conexaoEngine()
        consulta = pd.read_sql(sql,conn)

        return consulta

    def estoqueComprometido(self):

        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        caminho_absoluto = configApp.localProjeto

        consumo = pd.read_csv(f'{caminho_absoluto}/dados/requisicoesEmAberto.csv')

        consumo.drop(['Unnamed: 0'], axis=1, inplace=True)
        consumo['CodComponente'] = consumo['CodComponente'].astype(str)

        return consumo

    def estoquePedidosCompras(self):

        # 1:  Carregar as variaveis de ambiente e o nome do caminho
        caminho_absoluto = configApp.localProjeto
        consumo = pd.read_csv(f'{caminho_absoluto}/dados/pedidosEmAberto.csv')

        consumo.drop(['Unnamed: 0'], axis=1, inplace=True)
        consumo['CodComponente'] = consumo['CodComponente'].astype(str)
        consumo['numero'] = consumo['numero'].astype(str)

        consumo.drop(['fatCon','sitSugestao','seqitem','qtAtendida','qtdPedida','sitSugestao'], axis=1, inplace=True)

        return consumo













