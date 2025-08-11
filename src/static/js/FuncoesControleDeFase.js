const InputPlano = document.getElementById("InputCodPlanoCurva");
let TextoFases = "";
let TextoDescricaoFase = "";
let TextoResponsavelFase = "";
let IndiceExibicao = 0;
let TotalPaginas = 0;
let PaginaAtual = 1;
const itensPag = 15;



function ObterFases() {
  const dados = {}
  fetch(`http://192.168.0.183:8000/pcp/api/ResponsabilidadeFase`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'a44pcp22'
    },
    body: JSON.stringify(dados)
  })
    .then(response => {
      if (response.ok) {
        return response.json();
      } else {
        console.log(response);
        throw new Error('Erro ao obter a lista de usuários');
      }
    })
    .then(data => {
      console.log(data);
      CriarTabelaFases(data)
    })
    .catch(error => {
      console.error('Erro capturado:', error);
    });
};

function CriarTabelaFases(listaFases) {
  const tabela = document.getElementById('TabelaFases');
  tabela.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

  // Cria o cabeçalho da tabela
  const cabecalho = document.createElement('thead');
  const cabecalhoRow = document.createElement('tr');
  const ColunaCodFase = document.createElement('th');
  inputFase = document.createElement('input');
  inputFase.id = 'inputFase';
  //inputFase.setAttribute('placeholder', TextoFase);
  inputFase.addEventListener('keydown', event => FiltroFases(event));
  const ColunaDescricaoFase = document.createElement('th');
  inputDescricaoFase = document.createElement('input');
  inputDescricaoFase.id = 'inputDescricaoFase';
  //inputDescricaoFase.setAttribute('placeholder', TextoDescricaoFase);
  inputDescricaoFase.addEventListener('keydown', event => FiltroDescricoesFases(event));
  const ColunaResponsavelFases = document.createElement('th');
  inputResponsaveis = document.createElement('input');
  inputResponsaveis.id = 'inputResponsaveis';
  //inputResponsaveis.setAttribute('placeholder', TextoResponsavelFase);
  inputResponsaveis.addEventListener('keydown', event => FiltroResponsaveis(event));


  ColunaCodFase.textContent = 'Cód. Fase';
  ColunaDescricaoFase.textContent = 'Descrição Fase';
  ColunaResponsavelFases.textContent = 'Responsável Fase';


  ColunaCodFase.style.width = '120px';
  ColunaDescricaoFase.style.width = '200px';
  ColunaResponsavelFases.style.width = '150px';


  ColunaCodFase.appendChild(inputFase);
  ColunaDescricaoFase.appendChild(inputDescricaoFase);
  ColunaResponsavelFases.appendChild(inputResponsaveis);
  cabecalhoRow.appendChild(ColunaCodFase);
  cabecalhoRow.appendChild(ColunaDescricaoFase);
  cabecalhoRow.appendChild(ColunaResponsavelFases);
  cabecalho.appendChild(cabecalhoRow);
  tabela.appendChild(cabecalho);


  // Preenche a tabela com os dados da estrutura
  listaFases.forEach(item => {
    const row = document.createElement('tr');
    const ColunaCodFase = document.createElement('td');
    const ColunaDescricaoFase = document.createElement('td');
    const ColunaResponsavelFases = document.createElement('td');


    ColunaCodFase.textContent = item['codFase'];
    ColunaDescricaoFase.textContent = item['nomefase'];
    const inputResponsavel = document.createElement('input');
    inputResponsavel.type = 'text';
    inputResponsavel.value = item['responsavel'];
    inputResponsavel.addEventListener('input', event => AtualizarResponsavel(event, item));

    ColunaResponsavelFases.appendChild(inputResponsavel);


    row.appendChild(ColunaCodFase);
    row.appendChild(ColunaDescricaoFase);
    row.appendChild(ColunaResponsavelFases);
    tabela.appendChild(row);
  });

};

window.addEventListener('load', ObterFases);



const buttonSalvar = document.getElementById('ButtonSalvar');

buttonSalvar.addEventListener('click', function () {
  console.log('Botão Salvar clicado');
  const tabela = document.getElementById("TabelaFases");
  const linhasTabela = tabela.querySelectorAll("tr");

  const codFaseList = [];
  const responsavelFaseList = [];

  linhasTabela.forEach(row => {
    const codFase = row.cells[0].textContent;
    const responsavelFase = row.cells[2].querySelector('input').value;

    codFaseList.push(codFase);
    responsavelFaseList.push(responsavelFase);
  });

  codFaseList.shift();
  responsavelFaseList.shift();

  console.log(codFaseList);
  console.log(responsavelFaseList);

  // Monta um array de objetos com os dados para envio
  const dadosParaEnviar = 
       {
      codFase: codFaseList,
      nome: responsavelFaseList
      }

  

  // Envia os dados para a API
  fetch('http://192.168.0.183:8000/pcp/api/ResponsabilidadeFase', {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'a44pcp22'
    },
    body: JSON.stringify(dadosParaEnviar)
  })
    .then(response => response.json())
    .then(resultado => {
      console.log(resultado);
      alert("Dados Salvos com Sucesso")
    })
    .catch(error => {
      console.error('Erro ao enviar dados para a API:', error);
      alert("Dados não foram Salvos, contacte o Administrador")
    });
});

