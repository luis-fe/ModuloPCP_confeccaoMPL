'''Service da meta mensal do indice de 2a Qualidade'''
import logging
from datetime import datetime

import pytz

from src.models.Meta_Qualidade import Meta_Qualidade

logger = logging.getLogger(__name__)


class Meta_Qualidade_Service():
    '''
    Classe responsavel por orquestrar a meta mensal de 2a Qualidade no formato
    consumido pela tela de Gestao da Qualidade (requests.php do projeto Qualidade.php):

        Consulta -> { "AnoMeta": 2026, "Meses": [12 nomes], "Meta": [12 fracoes] }
        Gravacao -> { "status": bool, "message": str, "dados": <formato da consulta> }

    A "Meta" trafega como FRACAO (0.015 = 1,50%), igual ao que a tela envia e espera.
    A conversao para o percentual gravado na coluna numeric(10,2) é feita pelo model.
    '''

    ANO_MINIMO = 2000
    ANO_MAXIMO = 2100

    def __init__(self, ano=''):
        self.ano = str(ano)

    def consultar_ano(self):
        '''
        Metodo publico que devolve as 12 metas do ano. Mes sem meta cadastrada volta
        com a meta padrao (1,50%), mesmo comportamento do painel de indice.

        :return:
            dict no formato { 'AnoMeta', 'Meses', 'Meta' }
        '''
        # Ano ausente ou fora da faixa cai no ano corrente, como no requests.php
        self.ano = self.__validar_ano(self.ano) or self.__ano_corrente()

        consulta = Meta_Qualidade(self.ano).consultar_metas_ano()

        cadastradas = {}
        for _, linha in consulta.iterrows():
            mes = Meta_Qualidade.resolver_mes(linha['mes'])

            if mes is None:
                logger.warning('Mes fora do padrao na tabela MetaQualide: %s', linha['mes'])
                continue

            cadastradas[mes] = float(linha['meta'])

        metas = [
            Meta_Qualidade.percentual_para_fracao(
                cadastradas.get(mes, Meta_Qualidade.META_PADRAO)
            )
            for mes in Meta_Qualidade.MESES
        ]

        return {
            'AnoMeta': int(self.ano),
            'Meses': [Meta_Qualidade.nome_do_mes(mes) for mes in Meta_Qualidade.MESES],
            'Meta': metas
        }

    def salvar_ano(self, dados):
        '''
        Metodo publico que grava as 12 metas do ano.

        :param dados: dict no formato enviado pela tela - AnoMeta (int),
                      Meses (12 nomes) e Meta (12 fracoes, 0.015 = 1,50%)
        :return:
            dict no formato { 'status', 'message', 'dados' }
        '''
        dados = dados or {}

        ano = self.__validar_ano(dados.get('AnoMeta'))
        if ano is None:
            return {'status': False, 'message': 'Ano invalido.'}

        self.ano = ano

        metas = dados.get('Meta')
        if not isinstance(metas, list) or len(metas) != len(Meta_Qualidade.MESES):
            return {'status': False, 'message': 'É esperada uma meta para cada um dos 12 meses.'}

        meses_recebidos = dados.get('Meses')
        if not isinstance(meses_recebidos, list):
            meses_recebidos = []

        gravar = []
        for indice, valor in enumerate(metas):
            percentual = self.__validar_meta(valor)
            if percentual is None:
                return {
                    'status': False,
                    'message': 'Meta invalida: informe numeros entre 0 e 1 (0,015 = 1,50%).'
                }

            nome_recebido = meses_recebidos[indice] if indice < len(meses_recebidos) else ''
            mes = Meta_Qualidade.resolver_mes(nome_recebido, indice)

            gravar.append((mes, percentual))

        for mes, percentual in gravar:
            Meta_Qualidade(self.ano, mes, percentual).inserir_ou_atualizar_meta()

        return {
            'status': True,
            'message': 'Metas salvas.',
            'dados': self.consultar_ano()
        }

    @staticmethod
    def __ano_corrente():
        '''Metodo privado que devolve o ano corrente no fuso do Brasil'''
        fuso_horario = pytz.timezone('America/Sao_Paulo')

        return str(datetime.now(fuso_horario).year)

    def __validar_ano(self, ano):
        '''Metodo privado que valida o ano recebido e devolve-o como string'''
        try:
            ano = int(ano)
        except (TypeError, ValueError):
            return None

        if ano < self.ANO_MINIMO or ano > self.ANO_MAXIMO:
            return None

        return str(ano)

    @staticmethod
    def __validar_meta(valor):
        '''
        Metodo privado que valida a meta recebida em fracao e devolve o percentual
        a ser gravado (ou None quando o valor é invalido)
        '''
        try:
            valor = float(valor)
        except (TypeError, ValueError):
            return None

        if valor < 0 or valor > 1:
            return None

        return Meta_Qualidade.fracao_para_percentual(valor)
