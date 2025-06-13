import requests

class Produtos_colorBook():

    def __init__(self, codItemPai, codColecao):

        self.codItemPai = codItemPai
        self.codColecao = codColecao


    def obtendoImagemColorBook(self):
        '''Metodo que obtem a imagem do colocr book '''

        url = f"http://app.grupompl.com.br/main/fotos/{self.codColecao}/referencias/P_{self.codItemPai}.jpg"
        response = requests.get(url)

        return response