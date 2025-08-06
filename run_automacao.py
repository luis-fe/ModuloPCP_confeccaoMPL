import os
import sys
import psutil
from src.models import Componentes_Csw



if __name__ == '__main__':
    PID = os.getpid()
    print('inicio servico automacao')
    tempo = 60*1*60
    Componentes_Csw.Componentes_CSW('1',tempo).inserirComponentesVariaveis()





    os.system('clear')


    # Iniciar nova instância do script após N segundos
    new_process = f"{sys.executable} {sys.argv[0]}"
    print(f'gerado o process {new_process}')
    os.system(f"sleep 120 && {new_process} &")
    #Encerrando o Registro de controle do PID
    p = psutil.Process(PID)
    p.terminate()


