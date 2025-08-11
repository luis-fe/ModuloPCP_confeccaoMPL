const InputPlano = document.getElementById("InputCodPlanoCurva");
let TextoEngenharia = "";
let TextoDescricao = "";
let TextoCategoria = "";
let TextoMarca = "";
let IndiceExibicao = 0;
let TotalPaginas = 0;
let PaginaAtual = 1;
const itensPag = 15;

function CriarTabelaEstrutura(listaCurva) {
    const tabela = document.getElementById('TabelaCurva');
    tabela.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const ColunaEngenharia = document.createElement('th');
    inputEngenharia = document.createElement('input');
    inputEngenharia.id = 'inputEngenharia';
    inputEngenharia.setAttribute('placeholder', TextoEngenharia);
    inputEngenharia.addEventListener('keydown', event => FiltroEngenharias(event));
    const ColunaDescricao = document.createElement('th');
    inputDescricao = document.createElement('input');
    inputDescricao.id = 'inputDescricao';
    inputDescricao.setAttribute('placeholder', TextoDescricao);
    inputDescricao.addEventListener('keydown', event => FiltroDescricoes(event));
    const ColunaCategoria = document.createElement('th');
    inputCategoria = document.createElement('input');
    inputCategoria.id = 'inputCategoria';
    inputCategoria.setAttribute('placeholder', TextoCategoria);
    inputCategoria.addEventListener('keydown', event => FiltroCategorias(event));
    const ColunaPedida = document.createElement('th');
    const ColunaFaturada = document.createElement('th');
    const ColunaABC = document.createElement('th');
    const ColunaCategoriaABC = document.createElement('th');
    const ColunaMarca = document.createElement('th');
    inputMarca = document.createElement('input');
    inputMarca.id = 'inputMarca';
    inputMarca.setAttribute('placeholder', TextoMarca);
    inputMarca.addEventListener('keydown', event => FiltroMarcas(event));



    ColunaEngenharia.textContent = 'Engenharia';
    ColunaDescricao.textContent = 'Descrição';
    ColunaCategoria.textContent = 'Categoria';
    ColunaPedida.textContent = 'Qtd. Pedida';
    ColunaFaturada.textContent = 'Qtd. Faturada';
    ColunaABC.textContent = 'Class. ABC';
    ColunaCategoriaABC.textContent = 'Class. Categoria ABC';
    ColunaMarca.textContent = 'Marca';

    ColunaEngenharia.style.width = '120px';
    ColunaDescricao.style.width = '200px';
    ColunaCategoria.style.width = '150px';
    ColunaPedida.style.width = '80px';
    ColunaFaturada.style.width = '80px';
    ColunaABC.style.width = '80px';
    ColunaCategoriaABC.style.width = '80px';
    ColunaMarca.style.width = '100px';


    ColunaEngenharia.appendChild(inputEngenharia);
    ColunaDescricao.appendChild(inputDescricao);
    ColunaCategoria.appendChild(inputCategoria);
    ColunaMarca.appendChild(inputMarca);
    cabecalhoRow.appendChild(ColunaEngenharia);
    cabecalhoRow.appendChild(ColunaDescricao);
    cabecalhoRow.appendChild(ColunaCategoria);
    cabecalhoRow.appendChild(ColunaPedida);
    cabecalhoRow.appendChild(ColunaFaturada);
    cabecalhoRow.appendChild(ColunaABC);
    cabecalhoRow.appendChild(ColunaCategoriaABC);
    cabecalhoRow.appendChild(ColunaMarca);
    cabecalho.appendChild(cabecalhoRow);
    tabela.appendChild(cabecalho);


    const Teste1 = listaCurva.slice(IndiceExibicao, IndiceExibicao + itensPag);

    // Preenche a tabela com os dados da estrutura
    Teste1.forEach(item => {
        const row = document.createElement('tr');
        const ColunaEngenharia = document.createElement('td');
        const ColunaDescricao = document.createElement('td');
        const ColunaCategoria = document.createElement('td');
        const ColunaPedida = document.createElement('td');
        const ColunaFaturada = document.createElement('td');
        const ColunaABC = document.createElement('td');
        const ColunaCategoriaABC = document.createElement('td');
        const ColunaMarca = document.createElement('td');

        ColunaEngenharia.textContent = item['engenharia'];
        ColunaDescricao.textContent = item['descricao'];
        ColunaCategoria.textContent = item['categoria'];
        ColunaPedida.textContent = item['qtdePedida'];
        ColunaFaturada.textContent = item['qtdeFaturada'];
        ColunaABC.textContent = item['classABC'];
        ColunaCategoriaABC.textContent = item['classABC_Cat'];
        ColunaMarca.textContent = item['MARCA'];

        row.appendChild(ColunaEngenharia);
        row.appendChild(ColunaDescricao);
        row.appendChild(ColunaCategoria);
        row.appendChild(ColunaPedida);
        row.appendChild(ColunaFaturada);
        row.appendChild(ColunaABC);
        row.appendChild(ColunaCategoriaABC);
        row.appendChild(ColunaMarca);
        tabela.appendChild(row);
    });

}

const labelPaginaCurva = document.getElementById('LabelContagemCurva');
const ModalLoadingCurva = document.getElementById('ModalLoadingCurva');
const LoaderCurva = document.getElementById('loaderCurva');


function AbrirLoadingCurva() {
    ModalLoadingCurva.style.display = "block";
    LoaderCurva.style.display = "flex";
}

function FecharLoadingCurva() {
    ModalLoadingCurva.style.display = "none";
    LoaderCurva.style.display = "none"
}


function ObterCurva(dados) {
    AbrirLoadingCurva();
    const divPaginamento = document.querySelector('.PaginamentoCurva');

    fetch(`http://192.168.0.183:8000/pcp/api/RankingABCVendas`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a44pcp22'
        },
        body: JSON.stringify(dados),
    })
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error('Erro ao obter a lista de usuários');
            }
        })
        .then(data => {
            console.log(data);
            const detalhamentoCurva = data[0]["1- Dados:"];
            const detalhamentoPaginas = data[0]["0-numero de paginas"];
            console.log(detalhamentoCurva);
            TotalPaginas = detalhamentoPaginas;
            labelPaginaCurva.textContent = `Página ${PaginaAtual} de ${TotalPaginas}`;
            CriarTabelaEstrutura(detalhamentoCurva);
            divPaginamento.style.display = "flex";
            FecharLoadingCurva(); // Coloquei aqui para fechar a modal após carregar os dados

        })
        .catch(error => {
            console.error(error);
            FecharLoadingCurva(); // Remova esta linha do bloco catch
        });
};




InputPlano.addEventListener("keydown", event => {
    const dadosIniciais = {
        "plano": InputPlano.value,
        "itensPag": itensPag
    };
    if (event.key === "Enter") {
        event.preventDefault(); // Impede o comportamento padrão de submit de formulário
        console.log("teste")
        ObterCurva(dadosIniciais);
    }

});


const LabelEngenharia = document.getElementById('LabelEngenhariaCurva');
const LabelDescricao = document.getElementById('LabelDescricaoCurva');
const LabelCategoria = document.getElementById('LabelCategoriaCurva');
const LabelMarca = document.getElementById('LabelMarcaCurva');
const BotaoProximoCurva = document.getElementById('ButtonProximaPaginaCurva');

function dadosFiltros() {
    const dadosFiltros = {
        "plano": InputPlano.value,
        "itensPag": itensPag,
        "engenharia": TextoEngenharia,
        "descricao": TextoDescricao,
        "categoria": TextoCategoria,
        "MARCA": TextoMarca
    };

    ObterCurva(dadosFiltros);
}

function FiltroEngenharias(event) {
    if (event.key === "Enter") {
        event.preventDefault();
        const FiltroEngenharia = inputEngenharia.value;
        console.log(FiltroEngenharia);
        LabelEngenharia.textContent = FiltroEngenharia;
        PaginaAtual = 1
        BotaoProximoCurva.disabled = false
        TextoEngenharia = LabelEngenharia.textContent;

        dadosFiltros();
    }
};

function FiltroDescricoes(event) {
    if (event.key === "Enter") {
        event.preventDefault();
        const FiltroDescricao = inputDescricao.value;
        console.log(FiltroDescricao);
        LabelDescricao.textContent = FiltroDescricao;
        PaginaAtual = 1
        BotaoProximoCurva.disabled = false
        TextoDescricao = LabelDescricao.textContent;

        dadosFiltros();
    }
};

function FiltroCategorias(event) {
    if (event.key === "Enter") {
        event.preventDefault();
        const FiltroCategoria = inputCategoria.value;
        console.log(FiltroCategoria);
        LabelCategoria.textContent = FiltroCategoria;
        PaginaAtual = 1
        BotaoProximoCurva.disabled = false
        TextoCategoria = LabelCategoria.textContent;

        dadosFiltros();
    }
};

function FiltroMarcas(event) {
    if (event.key === "Enter") {
        event.preventDefault();
        const FiltroMarca = inputMarca.value;
        console.log(FiltroMarca);
        LabelMarca.textContent = FiltroMarca;
        PaginaAtual = 1
        BotaoProximoCurva.disabled = false
        TextoMarca = LabelMarca.textContent;

        dadosFiltros();
    }
};

const botaoPrimeiraPaginaCurva = document.getElementById('ButtonPrimeiraPaginaCurva');
const botatoPaginaAnteriorCurva = document.getElementById('ButtonPaginaAnteriorCurva');
const botaoProximaPaginaCurva = document.getElementById('ButtonProximaPaginaCurva');
const BotaoUltimaPaginaCurva = document.getElementById('ButtonUltimaPaginaCurva');

botaoProximaPaginaCurva.addEventListener("click", carregarProximaCurva);
BotaoUltimaPaginaCurva.addEventListener("click", CarregarUltimaPaginaCurva);
botatoPaginaAnteriorCurva.addEventListener("click", CarregarPaginaAnterior);
botaoPrimeiraPaginaCurva.addEventListener("click", CarregarPrimeiraPagina);




function carregarProximaCurva() {
    if (PaginaAtual + 1 <= TotalPaginas) {
        IndiceExibicao = 0;
        PaginaAtual = PaginaAtual + 1;
        const dadosProximo = {
            "plano": InputPlano.value,
            "itensPag": itensPag,
            "pagina": PaginaAtual,
            "engenharia": TextoEngenharia,
            "descricao": TextoDescricao,
            "categoria": TextoCategoria,
            "MARCA": TextoMarca
        };
        ObterCurva(dadosProximo);
    } else {
        alert("Ultima Página")
    }
};

function CarregarUltimaPaginaCurva() {
    if (PaginaAtual === TotalPaginas) {
        alert("Última Página");
    } else {
        IndiceExibicao = 0;
        PaginaAtual = TotalPaginas;
        const dadosUltimaPagina = {
            "plano": InputPlano.value,
            "itensPag": itensPag,
            "pagina": parseInt(PaginaAtual),
            "engenharia": TextoEngenharia,
            "descricao": TextoDescricao,
            "categoria": TextoCategoria,
            "MARCA": TextoMarca
        };
        ObterCurva(dadosUltimaPagina);
    };
};

function CarregarPaginaAnterior() {
    console.log("BOTAO ATIVADO");
    if (PaginaAtual > 1) {
        IndiceExibicao = 0;
        PaginaAtual = PaginaAtual - 1;
        const dadosPaginaAnterior = {
            "plano": InputPlano.value,
            "itensPag": itensPag,
            "pagina": PaginaAtual,
            "engenharia": TextoEngenharia,
            "descricao": TextoDescricao,
            "categoria": TextoCategoria,
            "MARCA": TextoMarca
        };
        ObterCurva(dadosPaginaAnterior);
    } else {
        alert("Primeira Página")
    }
}

function CarregarPrimeiraPagina() {
    if (PaginaAtual === 1) {
        alert("Primeira Página")
    } else {
        IndiceExibicao = 0;
        PaginaAtual = 1;
        const dadosUltimaPagina = {
            "plano": InputPlano.value,
            "itensPag": itensPag,
            "pagina": PaginaAtual,
            "engenharia": TextoEngenharia,
            "descricao": TextoDescricao,
            "categoria": TextoCategoria,
            "MARCA": TextoMarca
        };
        ObterCurva(dadosUltimaPagina);
    };
};


const BotaoPesquisaPlano = document.getElementById("ButtonPesquisaPlanoCurva");
const ModalPlanosCurva = document.getElementById("modalSelecionarPlanoCurva");

BotaoPesquisaPlano.addEventListener('click', function () {
    console.log("botão clicado")
    ModalPlanosCurva.style.display = "flex";
    ConsultaPlanosExistentes()
});

const fecharModalPlanosCurva = document.getElementById('fecharModalSelecaoPlanosCurva');
  

fecharModalPlanosCurva.addEventListener('click', function() {
    ModalPlanosCurva.style.display = 'none';
});

//------------------------------------------------TABELA DOS PLANOS-------------------------------------------------------------------//

function ConsultaPlanosExistentes() {
    fetch(`http://192.168.0.183:8000/pcp/api/Plano`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'a44pcp22'
      },
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
        criarTabelaPlanos(data)
        console.log(data);
      })
      .catch(error => {
        console.error('Erro capturado:', error);
      });
  }


function criarTabelaPlanos(listaPlanos) {
    const tabelaPlanos = document.getElementById('TabelaPlanosCurva');
    tabelaPlanos.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckboxPlanos = document.createElement('th');
    const ColunaCodigoPlano= document.createElement('th');
    const ColunaDescricaoPlanos = document.createElement('th');

    colunaCheckboxPlanos.textContent = '';
    ColunaCodigoPlano.textContent = 'Código';
    ColunaDescricaoPlanos.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckboxPlanos);
    cabecalhoRow.appendChild(ColunaCodigoPlano);
    cabecalhoRow.appendChild(ColunaDescricaoPlanos);
    cabecalho.appendChild(cabecalhoRow);
    tabelaPlanos.appendChild(cabecalho);

    listaPlanos.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckboxPlanos = document.createElement('td');
        const ColunaCodigoPlano = document.createElement('td');
        const ColunaDescricaoPlanos = document.createElement('td');

        const checkboxPlanos = document.createElement('input');
        checkboxPlanos.type = 'checkbox';
        checkboxPlanos.value = item["01- Codigo Plano"];
        checkboxPlanos.name = 'checkboxPlanos';
        colunaCheckboxPlanos.appendChild(checkboxPlanos);
        ColunaCodigoPlano.textContent = item["01- Codigo Plano"];
        ColunaDescricaoPlanos.textContent = item["02- Descricao do Plano"];

        row.appendChild(colunaCheckboxPlanos);
        row.appendChild(ColunaCodigoPlano);
        row.appendChild(ColunaDescricaoPlanos);

        tabelaPlanos.appendChild(row);
    });
};

const botaoSelecionarPlano = document.getElementById('botaoSelecionarPlanoCurva')
  
  botaoSelecionarPlano.addEventListener('click', function() {
  const LinhasTabelaPlano = document.getElementById('TabelaPlanosCurva').getElementsByTagName('tr');

  
      for (let i = 1; i < LinhasTabelaPlano.length; i++) {
          const linha2 = LinhasTabelaPlano[i];
          const checkboxPlanoSelecionado = linha2.querySelector('input[type="checkbox"]');
  
          if (checkboxPlanoSelecionado.checked) {
              const colunasPlanoSelecionado = linha2.getElementsByTagName('td');
              const Plano = colunasPlanoSelecionado[1].textContent.trim();
  
              plano = Plano
          }
      }
  
      if (plano.length === 0) {
          alert('Nenhuma Coleção selecionada');
          modalColecoes.style.display = 'none';
          modalColecoes.style.display = 'block';
      } else {
          let passandoDados = {
              "plano": plano,
              "itensPag": itensPag
          };
          console.log(plano)
          ModalPlanosCurva.style.display = 'none';
          InputPlano.value = plano
          ObterCurva(passandoDados);
        
      }
  
      modalPlanos.style.display = 'none';
  });

  //-----------------------------------------------Excel----------------------------------------------------------------------------


function exportarParaExcel (){

    const dadosParaExportar = {
      "plano": InputPlano.value,
      "Excel": true,
      "engenharia": TextoEngenharia,
      "descricao": TextoDescricao,
      "categoria": TextoCategoria,
      "MARCA": TextoMarca,
    };

    fetch("http://192.168.0.183:8000/pcp/api/RankingABCVendas", {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'a44pcp22'
      },
      body: JSON.stringify(dadosParaExportar),
    })
    .then(response => {
      if (response.ok) {
        return response.json();
      } else {
        throw new Error('Erro ao obter os dados da API');
      }
    })
    .then(data => {
      const detalhamentoCurva = data[0]['1- Dados:'];
      
      
      // Verificar se os dados foram obtidos corretamente
      if (!detalhamentoCurva || detalhamentoCurva.length === 0) {
        throw new Error('Não há dados para exportar.');
      }
      
      const nomeArquivo = 'Curva ABC.xlsx';
      const wb = XLSX.utils.book_new();
      const ws = XLSX.utils.json_to_sheet(detalhamentoCurva);
      
      // Adicionar a planilha ao workbook
      XLSX.utils.book_append_sheet(wb, ws, 'Dados Curva ABC');
  
      // Salvar o arquivo
      XLSX.writeFile(wb, nomeArquivo);
    })
    .catch(error => {
      console.error(error);
    });
};

  const botaoExportarExcel = document.getElementById('ButtonExportarExcelCurva');
  botaoExportarExcel.addEventListener('click', () => exportarParaExcel());


