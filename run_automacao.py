import os
from datetime import datetime
import pytz

# Imports do seu sistema
from src.service import Automacao_Service
from src.models import Componentes_Csw, Tags_apontadas_defeito_Csw, Pedidos_CSW, OrdemProd

def obter_hora_atual() -> str:
    """Retorna a hora atual no fuso horário de São Paulo formatada."""
    fuso_horario = pytz.timezone('America/Sao_Paulo')
    agora = datetime.now(fuso_horario)
    return agora.strftime('%Y-%m-%d %H:%M:%S')

def main():
    data = obter_hora_atual()
    print(f'Inicio servico automacao versao 04.05  - {data}')

    # Correção do Bug: Obtém a variável com um valor padrão seguro ('600') e converte para inteiro
    try:
        tempo_realizado_fases = int(os.getenv('freq_seg_realizado_fase', '600'))
    except ValueError:
        print("Aviso: 'freq_seg_realizado_fase' não é um número válido. Usaremos padrão 600.")
        tempo_realizado_fases = 600

    
    # Execução das rotinas
    pedidos_csw = Pedidos_CSW.Pedidos_CSW('1')
    pedidos_csw.put_automacao()

    Automacao_Service.Automacao().recebimento_aviamentos_CSW()

    # Dica: Substitua os parâmetros mágicos por variáveis explícitas no futuro
    ordem_prod_csw = OrdemProd.OrdemProd(
        '1', '', '', '', 100, tempo_realizado_fases
    )
    ordem_prod_csw.realizado_fases_csw()

    Automacao_Service.Automacao().buscar_informacao_aviamentos_disponiveis_CSW()


if __name__ == '__main__':
    main()