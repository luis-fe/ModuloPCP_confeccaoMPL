import os
import sys
from datetime import datetime

import psutil
import pytz

from src.models import Componentes_Csw, Tags_apontadas_defeito_Csw, Pedidos_CSW, OrdemProd


def obterHoraAtual():
    fuso_horario = pytz.timezone('America/Sao_Paulo')  # Define o fuso horário do Brasil
    agora = datetime.now(fuso_horario)
    agora = agora.strftime('%Y-%m-%d %H:%M:%S')
    return agora



if __name__ == '__main__':
    PID = os.getpid()
    data = obterHoraAtual()
    print(f'inicio servico automacao - {data}')
    tempo = 60*6*60
    tempo_tags = 60*10
    tempo_tags2 = 60*30
    tempo_realizadofases = 600

    Tags_apontadas_defeito_Csw.Tags_apontada_defeitos('1',tempo_tags, 20).inserindo_informacoes_tag_postgre()
    #Componentes_Csw.Componentes_CSW('1',tempo).inserirComponentesVariaveis()
    Tags_apontadas_defeito_Csw.Tags_apontada_defeitos('1',tempo_tags2, 20,'Tags Pilotos').get_tags_pilotos_csw()


    pedidosCsw = Pedidos_CSW.Pedidos_CSW('1')
    pedidosCsw.put_automacao()


    ordemProd_Csw = OrdemProd.OrdemProd('1','','','',100,int(tempo_realizadofases))
    ordemProd_Csw.realizado_fases_csw()



    os.system('clear')


    # Iniciar nova instância do script após N segundos
    new_process = f"{sys.executable} {sys.argv[0]}"
    print(f'gerado o process {new_process}')
    os.system(f"sleep 120 && {new_process} &")
    #Encerrando o Registro de controle do PID
    p = psutil.Process(PID)
    p.terminate()


