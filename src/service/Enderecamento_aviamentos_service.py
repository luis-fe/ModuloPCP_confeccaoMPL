from datetime import datetime

import pandas as pd
import pytz

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


        fila = Endereco_aviamento.Endereco_aviamento(self.codEmpresa).get_consulta_fila_recebimento()


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


    def desdobrar_etiqueta_item(self, sequencia):
        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '', self.codItem, '',self.qtd_reposta)

        # 1 - excluir etiqueta do estoque reposto
        endereco_aviamento.delete_item_reposto(sequencia)

        # 2 - incluir de volta na fila
        endereco_aviamento.update_item_fila_repor()


        return pd.DataFrame([{'Mensagem':'item reposto com sucesso', 'status':True}])



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

    def get_mapa_enderecos(self):

        consulta = Endereco_aviamento.Endereco_aviamento('', '', '', '').get_mapa_endereco()
        consulta.fillna('0',inplace=True)

        return consulta



    def update_endereco_item_unitario(self, enderecoCorrigido, sequencia, usuario, matricula):

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem,self.obterHoraAtual(),self.qtd_reposta)

        # 1 - validade se o endereco está vazio

        consulta = endereco_aviamento.get_consultar_endereco(enderecoCorrigido)

        if not consulta.empty:

            item_pesquisado = consulta['codItem'][0]

            if item_pesquisado == self.codItem:
                endereco_aviamento.update_item_endereco_controle_unitario(enderecoCorrigido, sequencia, usuario, matricula)

                return pd.DataFrame([{'Mensagem': 'Item atualizado com sucesso ', 'status': True}])









    def inserir_endereco_item_reposto_kit(self, enderecoCorrigido, sequencia, usuario, matricula):
        '''Método que inseri o item enderecado'''

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem,self.obterHoraAtual(),self.qtd_reposta)


        # 1 - validade se o endereco está vazio

        consulta = endereco_aviamento.get_consultar_endereco(enderecoCorrigido)

        if consulta.empty:

            endereco_aviamento.reposicao_item_endereco(enderecoCorrigido, sequencia, usuario, matricula)


            return pd.DataFrame([{'Mensagem': 'Item reposto com sucesso ', 'status': True}])

        else:

            item_pesquisado = consulta['codItem'][0]

            if item_pesquisado == self.codItem:
                endereco_aviamento.reposicao_item_endereco(enderecoCorrigido, sequencia, usuario, matricula)

                return pd.DataFrame([{'Mensagem': 'Item reposto com sucesso ', 'status': True}])

            else:

                return pd.DataFrame([{'Mensagem': 'Endereco Oculpado com outro item ', 'status': False}])



    def inserir_endereco_item_reposto_unidades(self, enderecoCorrigido,usuario,matricula, sequencia):
        '''Método que inseri o item enderecado'''

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem, self.obterHoraAtual(),
                                                                   self.qtd_reposta)


        # 1 - validade se o endereco está vazio

        consulta = endereco_aviamento.get_consultar_endereco(enderecoCorrigido)

        if consulta.empty:
            endereco_aviamento.reposicao_item_endereco(enderecoCorrigido, sequencia, usuario, matricula,'por unidade')

            return pd.DataFrame([{'Mensagem': 'Item reposto com sucesso ', 'status': True}])

        else:
            item_pesquisado = consulta['codItem'][0]
            if item_pesquisado == self.codItem:
                endereco_aviamento.reposicao_item_endereco(enderecoCorrigido, sequencia, usuario, matricula,
                                                           'por unidade')

                return pd.DataFrame([{'Mensagem': 'Item reposto com sucesso ', 'status': True}])
            else:

                return pd.DataFrame([{'Mensagem': 'Endereco Oculpado com outro item ', 'status': False}])


    def transferir_endereco(self):
        '''Metoddo utilizado para transferir endereco dos itens '''


    def get_consultar_endereco(self, endereco):
        '''Metodo que consulta o endereco'''

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

        endereco_aviamento.endereco = endereco

        validar =  endereco_aviamento.consulta_endereco_individual()

        if validar.empty:

            return pd.DataFrame([{'Mensagem':'Endereco nao existe', 'status':False}])
        else:

            endereco_aviamento = endereco_aviamento.get_consultar_endereco(endereco)

            return endereco_aviamento




    def update_endereco_kit(self):
        '''Metodo que faz o update no endereco do kit '''


    def update_endereco_unidade(self):
        '''Metodo que faz o update no endereco caso for unidade '''


    def inserir_atualizar_sequencia_codMaterial(self, sequencia):
        '''Método que inseri a ultima sequencia do material '''

        consulta = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem).atualiza_inserir__sequenciaitem(sequencia)

        return pd.DataFrame([{'status': True, 'Mensagem': 'Ultima sequencia inserida com sucesso'}])

    def devolver_ultima_sequencia_item(self):
        '''Metodo que devolve a ultima sequencia de kit de um determinado material'''

        consulta = Endereco_aviamento.Endereco_aviamento('', '', '', '',
                                                                   self.codItem).get_ultima_sequencia_item()

        if consulta.empty:

            return pd.DataFrame([{'sequencia':0, 'teste':''}])
        else:

            return pd.DataFrame([{'sequencia':int(consulta['sequencia'][0]), 'teste':''}])


    def produtividade_aviamentos(self,dataInicio, dataFim):

        '''Metodo que busca a produtividade do wms aviamentos '''

        endereco_aviamento = Endereco_aviamento.Endereco_aviamento()

        consulta = endereco_aviamento.produtividade_reposicao(dataInicio, dataFim)

        consulta2 = endereco_aviamento.get_conferencia_periodo(dataInicio, dataFim)


        dados = {
            '01-ProdutividadeReposicao': consulta.to_dict(orient='records'),
            '02-ProdutividadeConferente': consulta2.to_dict(orient='records')}


        return pd.DataFrame([dados])




    def obterHoraAtual(self):
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.now(fuso_horario)
        agora = agora.strftime('%Y-%m-%d %H:%M:%S')
        return agora







