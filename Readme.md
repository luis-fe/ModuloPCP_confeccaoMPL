# Modulo PCP para Confecao
    Cliente: Grupo MPL
    Framework: Flask - python
    Criador: Luís Fernando Gonçalves de Lima Machado
    Versao Produção: 1.0 /Homologado 06-06-2025

## 1.0 Objetivo do Projeto
    
        Um microserviço backend que conecta-se ao ERP da Empresa 
    e ao banco de dados Postgre que está vinculado a aplicação, 
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

## 3 Diagrama de Classes:![Diagrama de Classes.png](docsProject%2FDiagrama%20de%20Classes.png)

## 4 Recursos de Otimização  usando congelamento '.CSV':
        Esse projeto possue recursos de otimizacao usando api's que buscam arquivos .csv congelado.
        Essa estrátegia ajuda na performance do projeto e na otimizacao dos recurso. Abaixo é descrito 
    como isso ocorre no projeto:

##### Diretorio "/dados" : encontra-se os arquivos temporarios em csv que fazem parte do projeto. 
| Arquivo                                                                                                    | Descrição                                                                                                                                                                                                                      | API de Disparo                                                                                                     |
|------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------|
| /dados/tendenciaPlano{self.codPlano}.csv                                                                   | Congelado para o cálculo da Tendência do Plano a nível SKU. Utilizado para carregar simulações que utilizam esse plano.                                                                                                         | POST "{URL-BASE}/pcp/api/tendenciaSku"                                                                             |
| /dados/Simuacao_{self.nomeSimulacao}<br>_tenendicaPlano{self.codPlano}</br>_descontaQtdPedido_nao.csv</br> | Congelada a simulação baseada nos parâmetros de simulação x tendência. Utilizado para "Detalhar" itens nessa simulação.                                                                                                        | POST "{URL-BASE}/pcp/api/simulacaoProgramacao"                                                                     |
| /dados/requisicoesEmAberto.csv                                                                             | Congelado o retorno das requisições em aberto a nível de SKU. Utilizado para melhorar a performance. Gatilho nas APIs relacionadas ao cálculo da necessidade de matéria-prima.                                                 | POST "{URL-BASE}/pcp/api/AnaliseMateriaisPelaTendencia"                                                            |
| /dados/pedidosEmAberto.csv                                                                                 | Congelado o retorno dos pedidos em aberto a nível de SKU. Utilizado para melhorar a performance. Gatilho nas APIs relacionadas ao cálculo da necessidade de matéria-prima.                                                     | POST "{URL-BASE}/pcp/api/AnaliseMateriaisPelaTendencia"                                                            |
| /dados/EstruturacaoPrevisao{self.codPlano}.csv                                                             | Congelada a estrutura de matéria-prima x previsão antes do cálculo da necessidade. Reutilizado para performance nas APIs de cálculo de necessidade.                                                                             | POST "{URL-BASE}/pcp/api/DetalhaNecessidade"<br>POST "{URL-BASE}/pcp/api/AnaliseMateriaisPelaTendencia" (BODY: congelar:True) |
| /dados/EstruturacaoPrevisao<br>{self.codPlano}_Simulacao{self.nomeSimulacao}.csv</br>                      | Congelada a estrutura previsão x tendência x simulação antes do cálculo da necessidade simulada. Reutilizado nas APIs que atualizam o cálculo das necessidades simuladas.                                                      | POST "{URL-BASE}/pcp/api/DetalhaNecessidade" (BODY: nomeSimulacao: xxx)                                            |


            