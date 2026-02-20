import pandas as pd
from src.models import Endereco_aviamento, Produtos_CSW


class Enderecamento_aviamento():

    def __init__(self, codEmpresa='1', rua='', quadra='', posicao='', rua_final='', quadra_final='', posicao_final=''):
        self.codEmpresa = codEmpresa
        self.rua = rua
        self.quadra = quadra
        self.posicao = posicao

        # Salvando as variáveis finais para uso no método em massa
        self.rua_final = rua_final
        self.quadra_final = quadra_final
        self.posicao_final = posicao_final

        self.endereco = f'{self.rua}-{self.quadra}-{self.posicao}'

    def fila_itens_enderecar(self):
        '''Metodo que obtem do ERP a Fila de Itens a serem enderecados '''
        fila = Produtos_CSW.Produtos_CSW(self.codEmpresa).estoqueNat_aviamentos()
        return fila

    def get_enderecos(self):
        '''Metodo para obter os enderecos cadastrados '''
        consulta = Endereco_aviamento.Endereco_aviamento().get_enderecos()
        return consulta

    def _gerar_lista_intervalo(self, inicio, fim):
        '''Metodo auxiliar para criar a lista do 'for', tratando letras ou números'''
        if not fim:
            fim = inicio  # Se o final vier vazio, roda apenas para o inicial

        inicio_str, fim_str = str(inicio).strip(), str(fim).strip()

        if inicio_str.isdigit() and fim_str.isdigit():
            # Se for número, cria o range numérico e já aplica o zfill(2)
            return [str(i).zfill(2) for i in range(int(inicio_str), int(fim_str) + 1)]
        else:
            # Se for letra (ex: Rua A até D), converte usando a tabela ASCII
            return [chr(i) for i in range(ord(inicio_str.upper()), ord(fim_str.upper()) + 1)]

    def inserir_endereco(self):
        '''Metodo para inserir um endereco individual '''
        rua_f = self.rua.zfill(2) if isinstance(self.rua, str) and self.rua.isdigit() else self.rua
        posicao_f = self.posicao.zfill(2) if isinstance(self.posicao, str) and self.posicao.isdigit() else self.posicao
        quadra_f = self.quadra.zfill(2) if isinstance(self.quadra, str) and self.quadra.isdigit() else self.quadra

        enderecamento = Endereco_aviamento.Endereco_aviamento(self.endereco, rua_f, posicao_f, quadra_f)
        verifica = enderecamento.consulta_endereco_individual()

        if verifica.empty:
            enderecamento.insert_endereco()
            return pd.DataFrame([{'Mensagem': 'Endereco Incluido com sucesso', 'status': True}])
        else:
            return pd.DataFrame([{'Mensagem': 'Endereco ja existe', 'status': False}])

    def inserir_endereco_massa(self):
        '''Metodo para inserir os enderecos em massa usando cruzamento de laços for'''

        # 1. Gera as listas iteráveis para Rua, Quadra e Posição
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

                    # Mantive a sua ordem original de envio: (endereco, rua, posicao, quadra)
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