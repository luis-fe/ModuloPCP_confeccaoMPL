import pandas as pd
from src.models import Produtos_CSW
from src.conection import ConexaoPostgre


class Produtos():
    '''Classe utilizada para o gerenciamento dos produtos (sku)  que compoe o PCP Confeccao'''

    def __init__(self, codSku = None):

        self.codSku = codSku

    def obter_UltimoSkuCadastrosNoPCP(self):
        '''Metodo que obtem os skus cadastrados no banco postgres da aplicacao '''


        sql = """
        select max(codigo::int) as maximo from "PCP".pcp.itens_csw
        """

        conn = ConexaoPostgre.conexaoEngine()

        consulta = pd.read_sql(sql, conn)
        ultimoSku = consulta['maximo'][0]

        return ultimoSku

    def recarregar_novosSkus_noBanco(self):
        '''Metodo utilizado para recarregar novos Sku's no banco de dados Postgres para utilizar na Plantaforma PCP '''

        ultimoSku = self.obter_UltimoSkuCadastrosNoPCP()

        produtos_CSW = Produtos_CSW.Produtos_CSW('',ultimoSku)

        # 1 - Obter itens ainda nao cadastrados no postgre
        consulta_novosItens = produtos_CSW.get_itensFilhos_Novos_CSW()

        # 2 - Atribuir categoria
        consulta_novosItens['categoria'] = '-'
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('CAMISA', row['nome'], 'CAMISA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('POLO', row['nome'], 'POLO', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BATA', row['nome'], 'CAMISA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('TRICOT', row['nome'], 'TRICOT', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BONE', row['nome'], 'BONE', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('TSHIRT', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('REGATA', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BLUSAO', row['nome'], 'AGASALHOS', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BABY', row['nome'], 'CAMISETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('JAQUETA', row['nome'], 'JAQUETA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('CARTEIRA', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BONE', row['nome'], 'BONE', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('CINTO', row['nome'], 'CINTO', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('PORTA CAR', row['nome'], 'CARTEIRA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('CUECA', row['nome'], 'CUECA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('MEIA', row['nome'], 'MEIA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('SUNGA', row['nome'], 'SUNGA', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('SHORT', row['nome'], 'SHORT', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BERMUDA', row['nome'], 'BERMUDA M', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BRINDES', row['nome'], 'DISPLAY', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BRINDES', row['nome'], 'CARTAZ', row['categoria']), axis=1)
        consulta_novosItens['categoria'] = consulta_novosItens.apply(
            lambda row: self.definir_Categoria('BRINDES', row['nome'], 'ADESIVO', row['categoria']), axis=1)


        try:
            # Implantando no banco de dados do Pcp na tabela itens_csw
            ConexaoPostgre.Funcao_InserirOFF(consulta_novosItens, consulta_novosItens['codigo'].size, 'itens_csw', 'append')
        except:
            print('segue o baile ')
        return consulta_novosItens



    def definir_Categoria(self, contem, valorReferencia, valorNovo, categoria):
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








