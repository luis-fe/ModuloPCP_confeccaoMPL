'''Service do apontamento de defeito de 2a Qualidade - imagem em disco + registro no postgre'''
import base64
import binascii
import logging
import os
import re
import unicodedata
from datetime import datetime
from io import BytesIO

import pytz
from PIL import Image, UnidentifiedImageError

from src.configApp import configApp
from src.models.ApontamentoDefeito import ApontamentoDefeito

logger = logging.getLogger(__name__)


class ApontamentoDefeito_Service():
    '''
    Classe responsavel por orquestrar o apontamento de defeito de 2a Qualidade:
    grava a imagem no volume do servidor e registra a linha em pcp."ApntamentoDefeito"
    com o caminho do arquivo, para consulta posterior.

    NOME DO ARQUIVO (definido por "tipoInformacao"):
        porTag -> <codTag>_<motivoDefeito>.jpeg
        porOP  -> <op>_<motivoDefeito>.jpeg

    O arquivo é sempre gravado como JPEG em PASTA_IMAGENS. Como o nome é
    deterministico, um novo apontamento da mesma chave + mesmo motivo SOBRESCREVE
    a imagem anterior e o registro correspondente no banco é atualizado (nao
    duplica linha) - o "caminhoImg" é a chave natural da tabela.

    "dataHora" é o momento da gravacao (fuso America/Sao_Paulo); "dataApontamento"
    vem da API. Todos os demais campos sao opcionais e ficam com '-' por default.
    '''

    # Volume do servidor onde as imagens sao gravadas (/app/dados no Docker)
    PASTA_IMAGENS = f'{configApp.localProjeto}/dados'

    EXTENSAO = '.jpeg'

    # Qualidade do JPEG gravado - equilibra nitidez do defeito e tamanho do arquivo
    QUALIDADE_JPEG = 85

    # Valores aceitos em "tipoInformacao" -> coluna que da nome ao arquivo
    TIPOS_INFORMACAO = {
        'portag': ('porTag', 'codTag'),
        'tag': ('porTag', 'codTag'),
        'porop': ('porOP', 'op'),
        'op': ('porOP', 'op')
    }

    # Formatos aceitos em "dataApontamento" - normalizados para 'YYYY-MM-DD'
    FORMATOS_DATA = ['%Y-%m-%d', '%d/%m/%Y', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y %H:%M:%S',
                     '%Y-%m-%dT%H:%M:%S', '%Y/%m/%d']

    # Controle para rodar o create table if not exists uma vez por processo
    _tabela_verificada = False

    def __init__(self, tipoInformacao=''):
        '''
        :param tipoInformacao: 'porTag' ou 'porOP' - define o nome do arquivo
        '''
        self.tipoInformacao = tipoInformacao

    def registrar(self, dados, imagem):
        '''
        Metodo publico que grava a imagem do defeito e registra o apontamento.

        :param dados: dict com dataApontamento, referencia, cor, tam, op, codTag,
                      usuario, motivoDefeito e detalhamento (todos opcionais, '-' default)
        :param imagem: bytes da imagem, arquivo do request (FileStorage) ou string base64
        :return:
            dict { 'status', 'message', 'dados' } - "dados" traz o registro gravado
        '''
        dados = dados or {}

        tipo = self.__resolver_tipo()
        if tipo is None:
            return {
                'status': False,
                'message': 'tipoInformacao invalido: informe "porOP" ou "porTag".'
            }

        self.tipoInformacao, coluna_chave = tipo

        campos = self.__normalizar_campos(dados)

        chave = campos[coluna_chave]
        if chave == '-':
            return {
                'status': False,
                'message': f'O campo "{coluna_chave}" é obrigatorio quando '
                           f'tipoInformacao = "{self.tipoInformacao}".'
            }

        conteudo = self.__obter_bytes(imagem)
        if not conteudo:
            return {'status': False, 'message': 'Nenhuma imagem recebida.'}

        nome_arquivo = self.__nome_arquivo(chave, campos['motivoDefeito'])
        caminho = os.path.join(self.PASTA_IMAGENS, nome_arquivo)

        try:
            self.__gravar_imagem(conteudo, caminho)
        except (UnidentifiedImageError, ValueError):
            return {'status': False, 'message': 'Arquivo recebido nao é uma imagem valida.'}
        except OSError as erro:
            logger.exception('Falha ao gravar a imagem do apontamento de defeito')
            return {'status': False, 'message': f'Falha ao gravar a imagem: {erro}'}

        campos['caminhoImg'] = caminho
        campos['dataHora'] = self.__agora()

        self.__garantir_tabela()

        modelo = ApontamentoDefeito(**campos)

        # Mesma chave + mesmo motivo reutiliza o arquivo, logo atualiza o registro
        existente = modelo.atualizar_apontamento({
            coluna: campos[coluna]
            for coluna in ApontamentoDefeito.COLUNAS_ATUALIZAVEIS
        })

        if existente:
            operacao = 'atualizado'
        else:
            modelo.inserir_apontamento()
            operacao = 'inserido'

        return {
            'status': True,
            'message': f'Apontamento de defeito {operacao}.',
            'dados': self.__para_dict(campos, nome_arquivo)
        }

    def consultar(self, filtros=None):
        '''
        Metodo publico que consulta os apontamentos gravados.

        :param filtros: dict com data_inicio, data_fim, textoAvancado e/ou qualquer
                        coluna de igualdade (op, codTag, referencia, cor, tam,
                        usuario, motivoDefeito, dataApontamento, caminhoImg)
        :return:
            lista de dicts, do apontamento mais recente para o mais antigo
        '''
        filtros = filtros or {}

        consulta = ApontamentoDefeito(
            dataApontamento=self.__normalizar_data(filtros.get('dataApontamento', '-')),
            referencia=self.__texto(filtros.get('referencia', '-')),
            cor=self.__texto(filtros.get('cor', '-')),
            tam=self.__texto(filtros.get('tam', '-')),
            op=self.__texto(filtros.get('op', '-')),
            codTag=self.__texto(filtros.get('codTag', '-')),
            usuario=self.__texto(filtros.get('usuario', '-')),
            motivoDefeito=self.__texto(filtros.get('motivoDefeito', '-')),
            caminhoImg=self.__texto(filtros.get('caminhoImg', '-'))
        ).consultar_apontamentos(
            data_inicio=self.__normalizar_data(filtros.get('data_inicio', '-')),
            data_fim=self.__normalizar_data(filtros.get('data_fim', '-')),
            textoAvancado=filtros.get('textoAvancado', '')
        )

        apontamentos = []
        for _, linha in consulta.iterrows():
            registro = {coluna: linha[coluna] for coluna in ApontamentoDefeito.COLUNAS}

            registro['dataHora'] = self.__data_hora_texto(registro['dataHora'])
            registro['nomeArquivo'] = os.path.basename(str(registro['caminhoImg']))
            registro['imagemDisponivel'] = os.path.isfile(str(registro['caminhoImg']))

            apontamentos.append(registro)

        del consulta

        return apontamentos

    def caminho_imagem(self, dados):
        '''
        Metodo publico que resolve o caminho do arquivo de imagem de um apontamento,
        usado pelo endpoint que devolve a foto.

        Aceita o "caminhoImg" gravado no banco ou a combinacao
        tipoInformacao + codTag/op + motivoDefeito.

        :return:
            dict { 'status', 'message', 'caminho' }
        '''
        dados = dados or {}

        caminho = self.__texto(dados.get('caminhoImg', '-'))

        if caminho == '-':
            tipo = self.__resolver_tipo()
            if tipo is None:
                return {
                    'status': False,
                    'message': 'Informe "caminhoImg" ou tipoInformacao + codTag/op.'
                }

            self.tipoInformacao, coluna_chave = tipo

            chave = self.__texto(dados.get(coluna_chave, '-'))
            if chave == '-':
                return {'status': False, 'message': f'O campo "{coluna_chave}" é obrigatorio.'}

            caminho = os.path.join(
                self.PASTA_IMAGENS,
                self.__nome_arquivo(chave, self.__texto(dados.get('motivoDefeito', '-')))
            )

        if not self.__caminho_permitido(caminho):
            return {'status': False, 'message': 'Caminho de imagem invalido.'}

        if not os.path.isfile(caminho):
            return {'status': False, 'message': 'Imagem nao encontrada.'}

        return {'status': True, 'message': 'Imagem localizada.', 'caminho': caminho}

    def atualizar(self, caminhoImg, dados):
        '''
        Metodo publico que atualiza os campos de um apontamento ja gravado
        (identificado pelo "caminhoImg"). A imagem nao é alterada aqui - para
        trocar a foto basta reenviar o POST com a mesma chave e motivo.

        :return:
            dict { 'status', 'message', 'dados' }
        '''
        dados = dados or {}

        caminhoImg = self.__texto(caminhoImg)
        if caminhoImg == '-':
            return {'status': False, 'message': 'Informe o "caminhoImg" do apontamento.'}

        alteracoes = {}
        for coluna in ApontamentoDefeito.COLUNAS_EDITAVEIS:
            if coluna not in dados:
                continue

            if coluna == 'dataApontamento':
                alteracoes[coluna] = self.__normalizar_data(dados[coluna])
            else:
                alteracoes[coluna] = self.__texto(dados[coluna])

        if not alteracoes:
            return {'status': False, 'message': 'Nenhum campo editavel informado.'}

        atualizados = ApontamentoDefeito(caminhoImg=caminhoImg).atualizar_apontamento(alteracoes)

        if not atualizados:
            return {'status': False, 'message': 'Apontamento nao encontrado.'}

        return {
            'status': True,
            'message': 'Apontamento atualizado.',
            'dados': self.consultar({'caminhoImg': caminhoImg})
        }

    def excluir(self, caminhoImg, excluirImagem=True):
        '''
        Metodo publico que exclui o apontamento e, por default, tambem o arquivo
        de imagem do volume.

        :return:
            dict { 'status', 'message' }
        '''
        caminhoImg = self.__texto(caminhoImg)
        if caminhoImg == '-':
            return {'status': False, 'message': 'Informe o "caminhoImg" do apontamento.'}

        excluidos = ApontamentoDefeito(caminhoImg=caminhoImg).excluir_apontamento()

        if not excluidos:
            return {'status': False, 'message': 'Apontamento nao encontrado.'}

        if excluirImagem and self.__caminho_permitido(caminhoImg) and os.path.isfile(caminhoImg):
            try:
                os.remove(caminhoImg)
            except OSError:
                logger.exception('Falha ao remover a imagem %s', caminhoImg)

        return {'status': True, 'message': 'Apontamento excluido.'}

    def __para_dict(self, campos, nome_arquivo):
        '''
        Metodo privado que monta o registro devolvido pela API depois da gravacao.
        '''
        registro = {
            coluna: campos.get(coluna, '-')
            for coluna in ApontamentoDefeito.COLUNAS
        }

        registro['dataHora'] = self.__data_hora_texto(campos.get('dataHora'))
        registro['tipoInformacao'] = self.tipoInformacao
        registro['nomeArquivo'] = nome_arquivo
        registro['imagemDisponivel'] = os.path.isfile(str(campos.get('caminhoImg', '')))

        return registro

    def __resolver_tipo(self):
        '''
        Metodo privado que valida o "tipoInformacao" recebido.

        :return:
            tupla (tipo canonico, coluna que nomeia o arquivo) ou None
        '''
        texto = self.__sem_acento(str(self.tipoInformacao).strip().lower())
        texto = texto.replace('_', '').replace('-', '').replace(' ', '')

        return self.TIPOS_INFORMACAO.get(texto)

    def __normalizar_campos(self, dados):
        '''
        Metodo privado que aplica o default '-' em todos os campos opcionais e
        normaliza a "dataApontamento" recebida.
        '''
        campos = {
            coluna: self.__texto(dados.get(coluna, '-'))
            for coluna in ApontamentoDefeito.COLUNAS_EDITAVEIS
        }

        campos['dataApontamento'] = self.__normalizar_data(dados.get('dataApontamento', '-'))

        return campos

    def __nome_arquivo(self, chave, motivoDefeito):
        '''Metodo privado que monta <chave>_<motivoDefeito>.jpeg ja saneado'''
        return f'{self.__sanitizar(chave)}_{self.__sanitizar(motivoDefeito)}{self.EXTENSAO}'

    def __gravar_imagem(self, conteudo, caminho):
        '''
        Metodo privado que converte o conteudo recebido em JPEG e grava no volume.
        Qualquer formato de entrada suportado pelo Pillow (png, webp, bmp...) é
        aceito e convertido, ja que o nome do arquivo é sempre .jpeg.
        '''
        os.makedirs(self.PASTA_IMAGENS, exist_ok=True)

        with Image.open(BytesIO(conteudo)) as imagem:
            # JPEG nao suporta canal alfa / paleta - RGB cobre todos os casos
            imagem.convert('RGB').save(caminho, format='JPEG', quality=self.QUALIDADE_JPEG)

    def __caminho_permitido(self, caminho):
        '''
        Metodo privado que garante que o caminho esta dentro de PASTA_IMAGENS -
        protege os endpoints de leitura/exclusao contra path traversal.
        '''
        pasta = os.path.realpath(self.PASTA_IMAGENS)
        alvo = os.path.realpath(str(caminho))

        try:
            return os.path.commonpath([pasta, alvo]) == pasta
        except ValueError:
            # Caminhos sem raiz comum (outra unidade no Windows, relativo x absoluto)
            return False

    @staticmethod
    def __obter_bytes(imagem):
        '''
        Metodo privado que extrai os bytes da imagem recebida - arquivo do request
        (FileStorage), bytes puros ou string base64 (com ou sem prefixo data:).
        '''
        if imagem is None:
            return None

        if isinstance(imagem, (bytes, bytearray)):
            return bytes(imagem)

        if hasattr(imagem, 'read'):
            return imagem.read()

        if isinstance(imagem, str):
            conteudo = imagem.split(',')[-1].strip()

            try:
                return base64.b64decode(conteudo, validate=True)
            except (binascii.Error, ValueError):
                return None

        return None

    @classmethod
    def __garantir_tabela(cls):
        '''Metodo privado que cria a tabela na primeira gravacao do processo'''
        if cls._tabela_verificada:
            return

        try:
            ApontamentoDefeito().criar_tabela()
        except Exception:
            # A tabela pode existir e o usuario do banco nao ter permissao de DDL -
            # nesse caso o insert seguinte é quem vai acusar o problema real
            logger.exception('Falha ao garantir a tabela pcp."ApntamentoDefeito"')

        cls._tabela_verificada = True

    @staticmethod
    def __agora():
        '''Metodo privado que devolve o momento do apontamento no fuso do Brasil'''
        fuso_horario = pytz.timezone('America/Sao_Paulo')

        return datetime.now(fuso_horario).replace(tzinfo=None)

    @staticmethod
    def __data_hora_texto(valor):
        '''Metodo privado que formata o "dataHora" lido do banco para o frontend'''
        if valor is None or str(valor) in ('NaT', 'nan', ''):
            return '-'

        try:
            return valor.strftime('%Y-%m-%d %H:%M:%S')
        except AttributeError:
            return str(valor)

    @classmethod
    def __normalizar_data(cls, valor):
        '''
        Metodo privado que normaliza a data recebida para 'YYYY-MM-DD'.
        Valor ausente ou fora dos formatos aceitos volta como '-'.
        '''
        texto = cls.__texto(valor)

        if texto == '-':
            return '-'

        for formato in cls.FORMATOS_DATA:
            try:
                return datetime.strptime(texto, formato).strftime('%Y-%m-%d')
            except ValueError:
                continue

        logger.warning('dataApontamento fora dos formatos aceitos: %s', texto)

        return '-'

    @staticmethod
    def __texto(valor):
        '''Metodo privado que aplica o default '-' dos parametros opcionais'''
        if valor is None:
            return '-'

        texto = str(valor).strip()

        return texto if texto else '-'

    @classmethod
    def __sanitizar(cls, valor):
        '''
        Metodo privado que transforma o valor em um pedaco seguro de nome de arquivo:
        sem acento, sem separador de diretorio e sem espaco.
        '''
        texto = cls.__sem_acento(str(valor).strip())
        texto = re.sub(r'[^A-Za-z0-9]+', '-', texto).strip('-')

        return texto if texto else 'sem-informacao'

    @staticmethod
    def __sem_acento(texto):
        '''Metodo privado que remove acentos'''
        normalizado = unicodedata.normalize('NFKD', texto)

        return ''.join(caractere for caractere in normalizado if not unicodedata.combining(caractere))
