import requests
from bs4 import BeautifulSoup

class Produtos_colorBook():

    def __init__(self, codItemPai, codColecao, indice = 0):

        self.codItemPai = codItemPai
        self.codColecao = codColecao
        self.indice = indice


    def obtendoImagemColorBook(self):
        '''Metodo que obtem a imagem do colocr book '''

        url = f"http://app.grupompl.com.br/main/fotos/{self.codColecao}/referencias/P_{self.codItemPai}.jpg"
        response = requests.get(url)

        return response

    def obtendoImagem_S_ColorBook(self):
        '''Metodo que obtem a imagem do colocr book '''

        base_url = f"http://app.grupompl.com.br/main/fotos/{self.codColecao}/referencias/"
        try:
            # Pega o HTML da pasta
            response = requests.get(base_url)
            if response.status_code != 200:
                return {'erro': 'Não foi possível acessar o diretório de imagens.'}

            soup = BeautifulSoup(response.text, 'html.parser')

            # Encontra todos os links do tipo <a href="P_...jpg">
            arquivos = [a['href'] for a in soup.find_all('a', href=True) if
                        a['href'].startswith('P_') and a['href'].endswith('.jpg')]

            # Filtra os arquivos que contêm o código
            imagens_codigo = [f"{base_url}{nome}" for nome in arquivos if f"P_{self.codItemPai}" in nome]

            # Se não houver nenhuma imagem:
            if not imagens_codigo:
                return {'mensagem': f'Nenhuma imagem encontrada para o código {self.codItemPai}.'}
                # Se o índice for válido

            if 0 <= self.indice < len(imagens_codigo):
                return {
                    'codigo': self.codItemPai,
                    'indice': self.indice,
                    'total_imagens': len(imagens_codigo),
                    'imagem_url': imagens_codigo[self.indice]
                }
            else:
                return {
                    'mensagem': f"O código {self.codItemPai} contém {len(imagens_codigo)} imagem(ns). Índice {self.codItemPai} não encontrado."
                }

        except Exception as e:
            return {'erro': f'Ocorreu um erro: {str(e)}'}



