import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv

from src.configApp import configApp
from src.connection import ConexaoPostgre
from src.models import Pedidos, Produtos, Meta_Plano, Lote_Csw


class Tendencia_Plano_Materiais():
    """Classe que gerencia o processo de Calculo de Tendencia Analise de Materiais de um plano """

    def __init__(self, codEmpresa = '1', codPlano = '', consideraPedBloq = '', codLote=''):
        '''Contrutor da classe '''
        self.codEmpresa = codEmpresa
        self.codPlano = codPlano
        self.consideraPedBloq = consideraPedBloq
        self.codLote = codLote



    def estruturaItens(self, pesquisaPor = 'lote', arraySimulaAbc = 'nao', simula = 'nao'):

        if pesquisaPor == 'lote':
            consumo = Lote_Csw.Lote_Csw(self.codLote, self.codEmpresa)
            consumo = consumo.get_materiaPrima_loteProd_CSW()



        else:

            inPesquisa = self.estruturaPrevisao()
            if simula == 'nao':
                sqlMetas = TendenciasPlano.TendenciaPlano(self.codPlano, self.consideraBloqueado).tendenciaVendas('nao')
            else:
                sqlMetas = TendenciasPlano.TendenciaPlano(self.codPlano, self.consideraBloqueado).simulacaoProgramacao(arraySimulaAbc)
