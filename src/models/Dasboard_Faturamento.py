import os
import pytz
from dotenv import load_dotenv, dotenv_values

import pandas as pd
import datetime
import calendar
from src.models.Pedidos_CSW import Pedidos_CSW

''''  Classe responsavel pelo modelo de geraracao de relatorios de faturamento para os dashboards  '''
class Dashboard_Faturamento():

    def __init__(self, codEmpresa = '1', ano = ''):

        self.codEmpresa = codEmpresa
        self.ano = ano


    def geracao_faturamento_mes_atual_empresa(self):
        '''Metodo que gera o faturamento do mes atual'''
        dataInicio, dataFinal = self.__obterMesAtual()

        pedidos_csw1 = Pedidos_CSW(self.codEmpresa,'','','','','',dataInicio, dataFinal)
        faturamentoMesEmpresa = pedidos_csw1.faturamento_periodo_empresa()
        faturamentoMes = pedidos_csw1.faturamento_nota_semPedidos_empresa()

        retorno = pd.concat([faturamentoMesEmpresa, faturamentoMes])

        return retorno

    def __obterMesAtual(self):
        '''Metodo que obtem a dataInicio e dataFim do MesAtual'''

        hoje = datetime.date.today()

        # Primeiro dia do mês
        dataInicio = hoje.replace(day=1)

        # Último dia do mês
        ultimo_dia = calendar.monthrange(hoje.year, hoje.month)[1]
        dataFinal = hoje.replace(day=ultimo_dia)

        return dataInicio, dataFinal

    def __obterFimMesAnterior(self):
        """Método que obtém a data final do mês anterior ao mês atual"""

        hoje = datetime.date.today()

        # Primeiro dia do mês atual
        primeiro_dia_mes_atual = hoje.replace(day=1)

        # Último dia do mês anterior = um dia antes do primeiro dia do mês atual
        dataFinalMesAnterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)

        return dataFinalMesAnterior


    def __geracao_faturamento_antes_mes_atual(self):


        dataFinal = self.__obterFimMesAnterior()
        dataInicio = '2023-01-01'

        pedidos_csw1 = Pedidos_CSW(self.codEmpresa,'','','','','',dataInicio, dataFinal)
        faturamentoMesEmpresa = pedidos_csw1.faturamento_periodo_empresa()
        faturamentoMes = pedidos_csw1.faturamento_nota_semPedidos_empresa()

        retorno = pd.concat([faturamentoMesEmpresa, faturamentoMes])

        return retorno




    def backup_dados_empresa(self):
        caminhoAbsoluto = os.getenv('CAMINHO')


        consulta = self.__geracao_faturamento_antes_mes_atual()

        consulta.to_csv(f'{caminhoAbsoluto}/dados/{self.codEmpresa}-backup.csv')


    def __pedidos_retorna_empresa(self):
        '''Metodo que busca a fila de pedidos no retorna'''

        pedidos_csw_retorna = Pedidos_CSW(self.codEmpresa).retorna_csw_empresa()

        return pedidos_csw_retorna

    def __obterHoraAtual():
        fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
        agora = datetime.datetime.now(fuso_horario)
        hora_str = agora.strftime('%Y-%m-%d %H:%M:%S')
        dia = agora.strftime('%Y-%m-%d')
        return hora_str, dia

    def geracaoRelatorioAnual(self):
        datahora, dia = self.__obterHoraAtual()

        caminhoAbsoluto = os.getenv('CAMINHO')
        tipo_nota = Pedidos_CSW(self.codEmpresa).obtendoTipoNotaCsw()
        tipo_nota['codigo'] = tipo_nota['codigo'].astype(str)

        retornaCsw = self.__pedidos_retorna_empresa()
        retornaCsw = pd.merge(retornaCsw, tipo_nota, on='codigo')
        retornaCsw["codPedido"] = retornaCsw["codPedido"] + '-' + retornaCsw["codSequencia"]
        # Retirando as bonificacoes
        retornaCswSB = retornaCsw[retornaCsw['codigo'] != 39]
        retornaCswMPLUS = retornaCsw[retornaCsw['codigo'] == 39]
        retornaCswSB = retornaCswSB[retornaCswSB['conf'] == 0]
        retornaCswMPLUS = retornaCswMPLUS[retornaCswMPLUS['conf'] == 0]

        retorna = retornaCswSB['vlrSugestao'].sum()
        retorna = "{:,.2f}".format(retorna)
        retorna = str(retorna)
        retorna = 'R$ ' + retorna.replace(',', ';').replace('.', ',').replace(';', '.')

        ValorRetornaMplus = retornaCswMPLUS['vlrSugestao'].sum()
        ValorRetornaMplus = "{:,.2f}".format(ValorRetornaMplus)
        ValorRetornaMplus = str(ValorRetornaMplus)
        ValorRetornaMplus = 'R$ ' + ValorRetornaMplus.replace(',', ';').replace('.', ',').replace(';', '.')

        # 1 - Verificar se o ano selecionando ja terminou

        anoAtual = str(datetime.date.today().year)

        if self.ano == anoAtual:
            consulta1 = self.__geracao_faturamento_antes_mes_atual()
            consulta2 = pd.read_csv(f'{caminhoAbsoluto}/dados/{self.codEmpresa}-backup.csv')

            dataframe = pd.concat([consulta1, consulta2])

        else:
            dataframe = pd.read_csv(f'{caminhoAbsoluto}/dados/{self.codEmpresa}-backup.csv')

        # 2 - Devolver o DataFrame com as informacoes a nivel mensal

        meses = ['01-Janeiro', '02-Fevereiro', '03-Março', '04-Abril', '05-Maio', '06-Junho',
                 '07-Julho', '08-Agosto', '09-Setembro', '10-Outubro', '11-Novembro', '12-Dezembro']

        faturamento_por_mes = []
        acumulado = 0.00
        faturamento_acumulado = []


        for mes in meses:
            # Filtrar os dados do mês atual
            procura = f"-{mes.split('-')[0]}-"
            df_mes = dataframe[dataframe['dataEmissao'].str.contains(procura)]
            # Calcular o faturamento do mês
            faturamento_mes = df_mes['faturado'].sum()

            # Acumular o faturamento
            acumulado += faturamento_mes

            # Formatar o faturamento do mês
            faturamento_mes = "{:,.2f}".format(faturamento_mes)
            faturamento_mes = 'R$ ' + faturamento_mes.replace(',', ';').replace('.', ',').replace(';', '.')
            if faturamento_mes == 'R$ 0,00':
                faturamento_mes = ''

            # Formatar o acumulado
            acumulado_str = "{:,.2f}".format(acumulado)
            acumulado_str = 'R$ ' + acumulado_str.replace('.', ';')

            acumulado_str = acumulado_str.replace(',', '.')
            acumulado_str = acumulado_str.replace(';', ',')

            faturamento_por_mes.append(faturamento_mes)
            faturamento_acumulado.append(acumulado_str)

        # Criar um DataFrame com os resultados
        df_faturamento = pd.DataFrame(
            {'Mês': meses, 'Faturado': faturamento_por_mes, 'Fat.Acumulado': faturamento_acumulado})
        total = dataframe['faturado'].sum()
        total = "{:,.2f}".format(total)
        total = 'R$ ' + str(total)
        total = total.replace('.', ";")
        total = total.replace(',', ".")
        total = total.replace(';', ",")
        df_dia = dataframe[dataframe['dataEmissao'].str.contains(dia)]
        df_dia = df_dia['faturado'].sum()
        df_dia = "{:,.2f}".format(df_dia)
        df_dia = 'R$ ' + str(df_dia)
        df_dia = df_dia.replace('.', ";")
        df_dia = df_dia.replace(',', ".")
        df_dia = df_dia.replace(';', ",")





