# Modulo PCP para Confecao
    Cliente: Grupo MPL
    Tipo do Projeto: BackEnd
    Framework: Flask - Python
    Criador: Luís Fernando Gonçalves de Lima Machado
    Versao Produção: 1.2 /Homologado 23-06-2025

## Path Atualizacao

    Adicionado novas categorias de Materia Prima ao codigo;
    Adicionando o recurso de Monitor de Pre faturmanto ao BackEnd, class "MonitorPedidosOP"


## 1 Objetivo do Projeto
    
        Um microserviço backend que conecta-se ao ERP da Empresa 
    e ao banco de dados Postgre, que está vinculado a aplicação, 
    produzindo Api's com informações para projeto de PCP Confeccao. 

## 2 Inicializando o app
    
    1 - Configurar o projeto para o funcionamento: 
        
    1.1 variavéis de ambiente (ocultas) para Conexao: 
            criar arquivo _ambiente.env no diretorio PAI do projeto
    
    <Modelo ambiente_env:>

      POSTGRES_PASSWORD_SRV1=xxxx
      POSTGRES_PASSWORD_SRV2=xxxx
      POSTGRES_DB=xxxx
      POSTGRES_USER=xxxx
      POSTGRES_HOST_SRV1=xxx.xxx.x.xxx
      POSTGRES_HOST_SRV2=xxx.xxx.x.xxx
      POSTGRES_PORT = xxxx
      CSW_USER=xxxx
      CSW_PASSWORD=xxxx
      CSW_HOST=xxx.xxx.x.xxx
      CAMINHO_PARQUET_FAT=/xxxx/xxxx/xxxx/xxxx

    1.2 modificar a variavel global com o nome do local em ./src/configAPP,
    
    arquivo <configApp.py> :
        
        localProjeto = "xxxx/xxxx/xxxx"
    
    
        
    2 - Deploy da Aplicacao: 
        requeriments.txt
        app_run.py ("class main do projeto")
    
    2.1 - Alternativa via Docker: Dockerfile 

## 3 Diagrama de Classes:
#### Abaixo é detalhado o diagrama de classes, as classes que utilizam informacoes linkadas ao ERP da empresa, foram isoladas (destacadas em verde) para melhor manutencao.
#### Na esquerda do digrama foi destacada as rotas de API que sao consumidas no FrontEnd, no diretorio "/routes" do projeto.
#### ![Diagrama de Classes.png](docsProject%2FDiagrama%20de%20Classes.png)

## 4 Recursos de Otimização  usando congelamento '.CSV':
#### Esse projeto possue recursos de otimizacao usando api's que buscam arquivos .csv congelado.
#### Essa estrátegia ajuda na performance do projeto e na otimizacao dos recurso. Abaixo é descrito como isso ocorre no projeto:
##### ![Exemplo de Diretorio dados.png](docsProject%2FExemplo%20de%20Diretorio%20dados.png)
##### Diretorio "/dados" : encontra-se os arquivos temporarios em csv que fazem parte do projeto. 
| Arquivo                                                                                                    | Descrição                                                                                                                                                               | API's de Disparo                                                                                                                                |
|------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| /dados/tendenciaPlano-{self.codPlano}.csv                                                                  | Congelado para o cálculo da Tendência do Plano a nível SKU. Utilizado para carregar simulações que utilizam esse plano.                                                 | POST<br>"{URL-BASE}/pcp/api<br>/tendenciaSku"</br>                                                                                              |
| /dados/backup/{self.obterdiaAtual()}_tendenciaPlano-{self.codPlano}.csv                                    | Congelado para o backup de historico da Tendencia a nivel de dia                                                                                                        | POST<br>"{URL-BASE}/pcp/api                                                                                           |
| /dados/Simuacao_{self.nomeSimulacao}<br>_tendenciaPlano{self.codPlano}</br>_descontaQtdPedido_nao.csv</br> | Congelada para geracao de simulação baseada em parâmetros (%Simulação x tendência).<br>Pode ser Utilizada para "Detalhar" itens baseado na simulação.                   | POST<br>"{URL-BASE}/pcp/api<br>/simulacaoProgramacao"</br>                                                                                      |
| /dados/requisicoesEmAberto.csv                                                                             | Congelado para o retorno das requisições em aberto a nível de SKU. Objetivo é melhorar a performance.<br>Utilizada em Gatilhos nas API: GET .../pcp/api/comprometidoOP. | POST<br>"{URL-BASE}/pcp/api<br>/AnaliseMateriaisPelaTendencia"</br>                                                                             |
| /dados/pedidosEmAberto.csv                                                                                 | Congelado para o retorno dos pedidos em aberto a nível de SKU. Objetivo melhorar a performance.<br>Utilizada em Gatilhos nas API: GET .../pcp/api/comprometidoCompras.  | POST<br>"{URL-BASE}/pcp/api<br>/AnaliseMateriaisPelaTendencia"</br>                                                                             |
| /dados/EstruturacaoPrevisao<br>{self.codPlano}.csv</br>                                                    | Congelada para a performance na rotina de Analise de Materiais . Reutilizado-a nas APIs de detalhamento e congelamento da Analise .                                     | POST<br>"{URL-BASE}/pcp/api<br>/DetalhaNecessidade"<br></br>POST<br>"{URL-BASE}/pcp/api<br>/AnaliseMateriaisPelaTendencia" (BODY: congelar:True) |
| /dados/EstruturacaoPrevisao<br>{self.codPlano}</br>_Simulacao{self.nomeSimulacao}.csv</br>                 | Congelada para a performance na rotina de Analise de Materiais baseado em Simulação. Reutilizado-a nas APIs de detalhamento e congelamento da Analise (por Simulacao) . | POST<br>"{URL-BASE}/pcp/api<br>/DetalhaNecessidade" (BODY: nomeSimulacao: xxx)                                                                  |


            