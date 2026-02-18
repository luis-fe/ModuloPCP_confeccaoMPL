import numpy as np
import pandas as pd

from src.models import OrdemProd_Csw, UsuarioRequisicao, DashboardTV





class OrdemProd_service():
    def __init__(self, codEmpresa):

        self.codEmpresa = codEmpresa
        self.ordemProd_csw = OrdemProd_Csw.OrdemProd_Csw(self.codEmpresa)

    def ordemProd_requisicao_gerada(self):
        '''Metodo publico que busca as Ordem de Producao com requisicao em aberto '''

        ordemProd_aberto = self.ordemProd_csw.ordem_Prod_em_aberto()

        self.ordemProd_csw.codFase = '409'
        ordemProd_pos_fase = self.ordemProd_csw.ops_emAberto_movimentacao_fase()
        ordemProd_pos_fase['passou_separacao'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase, on='numeroOP', how='left')

        self.ordemProd_csw.codFase = '428'
        ordemProd_pos_fase2 = self.ordemProd_csw.ops_emAberto_movimentacao_fase()

        ordemProd_pos_fase2['passou_costura'] = 'sim'
        ordemProd_aberto = pd.merge(ordemProd_aberto, ordemProd_pos_fase2, on='numeroOP', how='left')

        ordemProd_aberto.fillna('-', inplace=True)

        # Lógica para criar a coluna situacaoOP
        ordemProd_aberto['situacaoOP'] = np.where(
            (ordemProd_aberto['passou_costura'] == '-') &
            (ordemProd_aberto['passou_separacao'] == 'sim'),
            'Em Operacao Almoxarifado',
            '-'
        )

        ordemProd_aberto = ordemProd_aberto[ordemProd_aberto['situacaoOP'] == 'Em Operacao Almoxarifado'].reset_index(
            drop=True)

        df_requisicoes = self.ordemProd_csw.requisicaoes_ops_em_aberto()
        df_requisicoes.fillna('-', inplace=True)

        # ---------------------------------------------------------
        # NOVA LÓGICA: Calcular a Situação Geral da OP
        # ---------------------------------------------------------

        # Substitua 'situacao' abaixo pelo nome real da coluna de status na sua tabela de requisições
        coluna_status_req = 'SITUACAO_REQUISICAO'

        def definir_status_geral(series):
            # Cria um conjunto com os status únicos encontrados para aquela OP
            status_unicos = set(series)

            # Remove '-' se houver, para não atrapalhar a lógica (opcional, dependendo da sua regra)
            status_unicos.discard('-')

            if status_unicos == {'BAIXADA'}:
                return 'BAIXADO'
            elif status_unicos == {'EM ABERTO'}:
                return 'EM ABERTO'
            else:
                # Se tiver misturado ou tiver outros status, cai aqui
                return 'PROCESSANDO'

        # Agrupa por OP e aplica a função de decisão
        df_situacao_calculada = (
            df_requisicoes.groupby('numeroOP')[coluna_status_req]
            .apply(definir_status_geral)
            .reset_index(name='SITUACAO_REQUISICAO')
        )

        # Faz o merge da nova coluna 'situacao' no DataFrame Pai
        ordemProd_aberto = pd.merge(ordemProd_aberto, df_situacao_calculada, on='numeroOP', how='left')

        # Preenche com algum valor padrão caso a OP não tenha requisições (ex: 'SEM REQUISICAO')
        ordemProd_aberto['SITUACAO_REQUISICAO'].fillna('SEM REQUISICAO', inplace=True)

        # ---------------------------------------------------------

        # Agrupamento original para criar a lista de dicionários (Mantido)
        requisicoes_agrupadas = (
            df_requisicoes.groupby('numeroOP')
            .apply(lambda x: x.to_dict('records'))
            .reset_index(name='requisicoes')
        )

        # Merge das listas de requisições com o DataFrame principal
        ordemProd_aberto = pd.merge(ordemProd_aberto, requisicoes_agrupadas, on='numeroOP', how='left')

        # Tratamento para garantir que seja uma lista
        ordemProd_aberto['requisicoes'] = ordemProd_aberto['requisicoes'].apply(
            lambda d: d if isinstance(d, list) else [])


        ordemProd_aberto = ordemProd_aberto[ordemProd_aberto['SITUACAO_REQUISICAO'] != 'BAIXADO'].reset_index(
            drop=True)

        return ordemProd_aberto


    def detalhar_requisicao(self, codRequisicao:str = '-'):

        requisicao =  self.ordemProd_csw.explodir_requisicao_op(codRequisicao)

        return requisicao

    def buscar_usuarios_habilitados(self):

        usuario = UsuarioRequisicao.Usuario_requisicao().get_usuarios_habilitados_req()

        return usuario


    def inserir_usuario_hablitado(self, codMatricula:str):
        descobrirNome = DashboardTV.DashboardTV('','','','',codMatricula).devolver_nome_usuario().reset_index()

        if descobrirNome.empty:
            return  pd.DataFrame([{'Mensagem':'Matricula nao encontrada', 'status':False}])

        else:
            nome = descobrirNome['nome'][0]
            usuario = UsuarioRequisicao.Usuario_requisicao(codMatricula,nome).habilitar_usuario_separacao()

            return usuario





