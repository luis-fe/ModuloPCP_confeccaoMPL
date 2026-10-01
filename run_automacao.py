import os
import traceback
from datetime import datetime
import pytz

# Imports do seu sistema
from src.service import Automacao_Service, PedidosVenda
from src.models import Componentes_Csw, Tags_apontadas_defeito_Csw, Pedidos_CSW, OrdemProd

def obter_hora_atual() -> str:
    """Retorna a hora atual no fuso horário de São Paulo formatada."""
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso_horario)
    return agora.strftime('%Y-%m-%d %H:%M:%S')

def executar_rotina(descricao: str, rotina):
    """Executa uma rotina isolada: se ela falhar, registra o erro e segue para a próxima."""
    try:
        rotina()
    except Exception:
        print(f'ERRO na rotina "{descricao}" - {obter_hora_atual()}')
        traceback.print_exc()

def main():
    data = obter_hora_atual()
    print(f'Inicio servico automacao versao 04.05  - {data}')

    # Correção do Bug: Obtém a variável com um valor padrão seguro ('600') e converte para inteiro
    try:
        tempo_realizado_fases = int(os.getenv('freq_seg_realizado_fase', '600'))
    except ValueError:
        print("Aviso: 'freq_seg_realizado_fase' não é um número válido. Usaremos padrão 600.")
        tempo_realizado_fases = 600

    #Automacao das tags apontadas como qualidade 2
    executar_rotina('Tags apontadas com defeito',
                    lambda: Tags_apontadas_defeito_Csw.Tags_apontada_defeitos().inserindo_informacoes_tag_postgre())

    # Automacao no dashboard TV
    executar_rotina('Dashboard TV', lambda: Pedidos_CSW.Pedidos_CSW('1').put_automacao())

    # Automacao na fila de recebimento de aviamentos
    executar_rotina('Fila de recebimento de aviamentos',
                    lambda: Automacao_Service.Automacao().recebimento_aviamentos_CSW())

    # Automacao do realizado fases
    executar_rotina('Realizado fases',
                    lambda: OrdemProd.OrdemProd('1', '', '', '', 100, tempo_realizado_fases).realizado_fases_csw())

    # Automacao dos aviamentos disponiveis do csw
    executar_rotina('Aviamentos disponiveis',
                    lambda: Automacao_Service.Automacao().buscar_informacao_aviamentos_disponiveis_CSW())

    # Automacao dos pedidos utilizando o arquivo .parquet
    executar_rotina('Pedidos (parquet)', lambda: PedidosVenda.Pedido_venda().incrementarPedidos())


if __name__ == '__main__':
    main()
