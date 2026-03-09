import pandas as pd
from src.models import Endereco_aviamento, Produtos_CSW, MateriaPrima


class Enderecamento_aviamento():

    def __init__(self, codEmpresa='1', rua='', quadra='', posicao='', rua_final='', quadra_final='', posicao_final='', coditem : str = '',
                 qtd_reposta: int = 0):
        self.codEmpresa = codEmpresa
        self.rua = rua
        self.quadra = quadra
        self.posicao = posicao
        self.codItem = coditem

        # Salvando as variáveis finais para uso no método em massa
        self.rua_final = rua_final
        self.quadra_final = quadra_final
        self.posicao_final = posicao_final

        self.endereco = f'{self.rua}-{self.quadra}-{self.posicao}'
        self.qtd_reposta = qtd_reposta

    def fila_itens_enderecar(self):
        '''Metodo que obtem do ERP a Fila de Itens a serem enderecados '''


        fila = Produtos_CSW.Produtos_CSW(self.codEmpresa).estoqueNat_aviamentos()

        # Verificando categorias
        categorias = MateriaPrima.Materia_prima_aviamento(self.codEmpresa).configuracao_de_para_descricao()

        # 2. Preparando os dados (Removendo acentos para não dar falha na busca)
        # ==========================================
        # Cria uma coluna temporária removendo acentos e deixando tudo maiúsculo
        fila['nome_limpo'] = fila['nome'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode(
            'utf-8').str.upper()

        fila['categoria'] = 'Outros'

        # ==========================================
        # 3. Aplicando as regras de categorização
        # ==========================================
        for _, regra in categorias.iterrows():
            gatilho = regra['descricao_contem'].upper()  # Garante que a regra está em maiúsculo
            categoria_destino = regra['categoria']

            # Se a coluna 'nome_limpo' contiver o gatilho, atualiza a coluna 'categoria'
            mascara_contem_gatilho = fila['nome_limpo'].str.contains(gatilho, na=False)

            fila.loc[mascara_contem_gatilho, 'categoria'] = categoria_destino

        # Remove a coluna temporária, já que não precisamos mais dela
        fila = fila.drop(columns=['nome_limpo'])

        def formatar_inteiro_milhar(valor):
            try:
                valor_inteiro = int(float(valor))
                # Formata com vírgula e substitui por ponto
                return f"{valor_inteiro:,}".replace(',', '.')
            except (ValueError, TypeError):
                return valor

        # Função para as demais unidades (Com casas decimais no padrão PT-BR)
        def formatar_decimal_ptbr(valor):
            try:
                valor_float = float(valor)

                # Formata no padrão americano mantendo 3 casas decimais (ex: 1,500.500)
                # Se quiser menos casas, basta mudar o '.3f' para '.2f', por exemplo
                formatado = f"{valor_float:,.3f}"

                # Inverte ponto e vírgula
                formatado = formatado.replace(',', '_')  # Troca a vírgula do milhar por '_'
                formatado = formatado.replace('.', ',')  # Troca o ponto decimal por ','
                formatado = formatado.replace('_', '.')  # Troca o '_' pelo ponto do milhar

                return formatado
            except (ValueError, TypeError):
                return valor

        # ==========================================
        # 3. Aplicando as Regras
        # ==========================================

        # Máscara para "UM"
        mascara_um = fila['unidadeMedida'] == 'UM'

        # Aplica a regra para "UM"
        fila.loc[mascara_um, 'estoqueAtual'] = fila.loc[mascara_um, 'estoqueAtual'].apply(formatar_inteiro_milhar)

        # Aplica a regra para o resto (onde a unidade NÃO É "UM")
        fila.loc[~mascara_um, 'estoqueAtual'] = fila.loc[~mascara_um, 'estoqueAtual'].apply(formatar_decimal_ptbr)

        fila['unidadeMedida'] = fila['unidadeMedida'].replace('UM','Unid')
        fila['unidadeMedida'] = fila['unidadeMedida'].replace('UN','Unid')

        fila.fillna('-',inplace=True)


        return fila

    def get_enderecos(self):
        '''Metodo para obter os enderecos cadastrados '''
        consulta = Endereco_aviamento.Endereco_aviamento().get_enderecos()
        return consulta

    def _gerar_lista_intervalo(self, inicio, fim):
        '''Metodo auxiliar para criar a lista do 'for', tratando letras ou números'''

        # 1. Trata casos onde a variável chega vazia, nula, ou como string "None"
        if inicio in [None, 'None', 'null', '']:
            return ['00']  # Retorna um valor padrão seguro para não quebrar o script

        if fim in [None, 'None', 'null', '']:
            fim = inicio  # Se o final vier vazio, roda apenas para o inicial

        inicio_str = str(inicio).strip()
        fim_str = str(fim).strip()

        # 2. Se for número puramente, cria o range numérico e aplica o zfill(2)
        if inicio_str.isdigit() and fim_str.isdigit():
            return [str(i).zfill(2) for i in range(int(inicio_str), int(fim_str) + 1)]

        # 3. Se for letra (ex: Rua A até D), pega APENAS a 1ª letra para o ord() não falhar
        else:
            char_inicio = inicio_str[0].upper()
            char_fim = fim_str[0].upper()
            return [chr(i) for i in range(ord(char_inicio), ord(char_fim) + 1)]

    def inserir_endereco(self):
        '''Metodo para inserir um endereco individual '''
        rua_f = self.rua.zfill(2) if isinstance(self.rua, str) and self.rua.isdigit() else self.rua
        posicao_f = self.posicao.zfill(2) if isinstance(self.posicao, str) and self.posicao.isdigit() else self.posicao
        quadra_f = self.quadra.zfill(2) if isinstance(self.quadra, str) and self.quadra.isdigit() else self.quadra

        endereco_f = f'{rua_f}-{quadra_f}-{posicao_f}'

        enderecamento = Endereco_aviamento.Endereco_aviamento(endereco_f, rua_f, posicao_f, quadra_f)
        verifica = enderecamento.consulta_endereco_individual()

        if verifica.empty:
            enderecamento.insert_endereco()
            return pd.DataFrame([{'Mensagem': 'Endereco Incluido com sucesso', 'status': True}])
        else:
            return pd.DataFrame([{'Mensagem': 'Endereco ja existe', 'status': False}])

    def inserir_endereco_massa(self):
        '''Metodo para inserir os enderecos em massa usando cruzamento de laços for'''

        # 1. Gera as listas iteráveis para Rua, Quadra e Posição usando a função segura
        ruas = self._gerar_lista_intervalo(self.rua, self.rua_final)
        quadras = self._gerar_lista_intervalo(self.quadra, self.quadra_final)
        posicoes = self._gerar_lista_intervalo(self.posicao, self.posicao_final)

        inseridos = 0
        ignorados = 0

        # 2. Laços 'for' aninhados para criar todas as combinações do intervalo
        for r in ruas:
            for q in quadras:
                for p in posicoes:

                    # Monta o endereço formatado da iteração atual
                    endereco_atual = f'{r}-{q}-{p}'

                    # Envia: (endereco, rua, posicao, quadra)
                    enderecamento = Endereco_aviamento.Endereco_aviamento(endereco_atual, r, p, q)
                    verifica = enderecamento.consulta_endereco_individual()

                    if verifica.empty:
                        enderecamento.insert_endereco()
                        inseridos += 1
                    else:
                        ignorados += 1

        # 3. Retorna um resumo inteligente do processamento em lote
        if inseridos > 0:
            msg = f'{inseridos} novos endereços inseridos com sucesso.'
            if ignorados > 0:
                msg += f' ({ignorados} ignorados por já existirem).'
            return pd.DataFrame([{'Mensagem': msg, 'status': True}])
        else:
            return pd.DataFrame(
                [{'Mensagem': f'Nenhum endereço criado. Todos os {ignorados} já existiam na base.', 'status': False}])

    def procurar_nome_item_considerar(self):

        # Busca o dataframe com os dados do material
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '', self.codItem).buscar_nomeMaterial()

        # Verifica se o DataFrame retornou vazio
        if endereco_aviamento.empty:
            endereco_aviamento['nomeMaterial'] = 'nao encontrado'
            # Usa .iloc[0] para acessar com segurança a primeira linha

        return endereco_aviamento

    def inserirItemDesconsiderar(self):
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('','','','',self.codItem).update_desconsidera_item_aviamento()

        return pd.DataFrame([{'Mensagem':'Item desativado com sucesso', 'status':True}])

    def remover_item_considerado(self):

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem).exluir_desconsidera_item_aviamento()

        return pd.DataFrame([{'Mensagem': 'Item excluido para nao  desconsiderar ', 'status': True}])

    def get_obter_itens_configurados(self):
        '''Metetodo que obtem os itens configurados '''

        consulta = Endereco_aviamento.Endereco_aviamento('', '', '', '').obter_itens_configuradados()

        return consulta


    def inserir_endereco_item_reposto_kit(self, enderecoCorrigido):
        '''Método que inseri o item enderecado'''

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem,'',self.qtd_reposta).reposicao_item_endereco()

        return pd.DataFrame([{'Mensagem': 'Item reposto com sucesso ', 'status': True}])


    def inserir_endereco_item_reposto_unidades(self):
        '''Método que inseri o item enderecado'''


    def transferir_endereco(self):
        '''Metoddo utilizado para transferir endereco dos itens '''


    def get_consultar_endereco(self):
        '''Metodo que consulta o endereco'''


    def update_endereco_kit(self):
        '''Metodo que faz o update no endereco do kit '''


    def update_endereco_unidade(self):
        '''Metodo que faz o update no endereco caso for unidade '''


    def inserir_produtividade_repositor(self):
        '''Método que inseri a produtividade do repositor '''






