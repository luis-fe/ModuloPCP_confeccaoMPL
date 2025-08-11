const ApiGetCurvaVenda = "http://192.168.0.183:8000/pcp/api/metaPlano";
const ApiMetaSemanal = "http://192.168.0.183:8000/pcp/api/metaPlanoSemanal";
const Token = "a44pcp22";
let ResultadoApi = [];
const MetaMpolloPcs = document.getElementById("PecasMpollo");
const MetaMpolloReais = document.getElementById("RSmpollo");
const MetaPacoPcs = document.getElementById("PecasPaco");
const MetaPacoReais = document.getElementById("RSpaco");
const TotalPcs = document.getElementById("PecasTotal");
const TotalReais = document.getElementById("RStotal");

function SomaTotais() {
  let valorMetaMpolloPcs = parseFloat(MetaMpolloPcs.value.replace(/\./g, '').replace(',', '.'));
  let valorMetaPacoPcs = parseFloat(MetaPacoPcs.value.replace(/\./g, '').replace(',', '.'));
  let valorMetaMpolloReais = parseFloat(MetaMpolloReais.value.replace('R$', '').replace(/\./g, '').replace(',', '.'));
  let valorMetaPacoReais = parseFloat(MetaPacoReais.value.replace('R$', '').replace(/\./g, '').replace(',', '.'));

  let SomaPecas = valorMetaMpolloPcs + valorMetaPacoPcs;
  let SomaValor = valorMetaMpolloReais + valorMetaPacoReais;

  // Formata os resultados para exibição
  SomaPecas = SomaPecas.toLocaleString();
  SomaValor = SomaValor.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });

  TotalPcs.value = SomaPecas;
  TotalReais.value = SomaValor;
}

function formatarValorInput(input) {
  let valor = input.value.replace(/\D/g, '');
  valor = (valor / 100).toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
  input.value = valor;
}

function Numerar(campo) {
  let Dados = document.getElementById(campo).value;
  // Remove tudo que não é dígito
  Dados = Dados.replace(/\D/g, '');
  // Formata o número com separador de milhar
  Dados = Dados.replace(/\B(?=(\d{3})+(?!\d))/g, '.');
  document.getElementById(campo).value = Dados;
}

document.getElementById("ButtonEditarCurvaDeVendas").addEventListener("click", async function () {
  const Plano = document.getElementById("InputPlano");
  document.getElementById("modalConfigVendas").style.display = "block";
  document.getElementById("InputPlanoMetas").value = Plano.value;
  document.getElementById("InputPlanoMetas").disabled = true;

  // Aguarde a conclusão da chamada da API usando await
  await ChamadaApi(ApiGetCurvaVenda, Plano.value);

  // Verifique se a resposta da API contém dados para a marca "M.POLLO"
  const MarcaMpollo = ResultadoApi.find(Resultado => Resultado.marca === "M.POLLO");
  if (MarcaMpollo) {
    MetaMpolloPcs.value = MarcaMpollo.Metapç;
    MetaMpolloReais.value = MarcaMpollo.MetaR$.replace("R$", "");
  } else {
    // Se não houver dados para "M.POLLO", defina os campos como vazios
    MetaMpolloPcs.value = "0";
    MetaMpolloReais.value = "0";
  }

  // Verifique se a resposta da API contém dados para a marca "PACO"
  const MarcaPaco = ResultadoApi.find(Resultado => Resultado.marca === "PACO");
  if (MarcaPaco) {
    MetaPacoPcs.value = MarcaPaco.Metapç;
    MetaPacoReais.value = MarcaPaco.MetaR$.replace("R$", "");
  } else {
    // Se não houver dados para "PACO", defina os campos como vazios
    MetaPacoPcs.value = "0";
    MetaPacoReais.value = "0";
  }

  SomaTotais();
});

async function ChamadaApi(api, codigo) {
  try {
    const response = await fetch(`${api}/${codigo}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'a44pcp22'
      },
    });

    if (response.ok) {
      const data = await response.json();
      ResultadoApi = data;
      console.log(ResultadoApi);
    } else {
      throw new Error('Erro No Retorno');
    }
  } catch (error) {
    console.error(error);
  }
}

function SalvarMeta(Marca, PecasMarca, ValorMarca) {
  // Obtenha os valores dos campos e faça as conversões necessárias

  // Prepare os dados a serem enviados
  const dadosEnviar = {
    codigoplano: parseInt(document.getElementById('InputPlanoMetas').value),
    marca: Marca,
    metaPeca: PecasMarca,
    metaReais: ValorMarca
  };
  console.log(dadosEnviar);

  // Faça a solicitação POST para a API
  fetch("http://192.168.0.183:8000/pcp/api/metaPlano", {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'a44pcp22'
    },
    body: JSON.stringify(dadosEnviar),
  })
    .then(response => {
      if (response.ok) {
        return response.json();
      } else {
        throw new Error('Erro ao obter os dados da API');
      }
    })
    .then(data => {
      console.log(data);
    })
    .catch(error => {
      console.error(error);
    });
}

document.getElementById("BotaoSalvarMetas").addEventListener("click", function () {
  const valorPecasPaco = parseInt(MetaPacoPcs.value.replace(/\./g, '').replace(',', '.'));
  const valorReaisPaco = parseFloat(MetaPacoReais.value.replace('R$', '').replace(/\./g, '').replace(',', '.'));
  const valorPecasMpollo = parseInt(MetaMpolloPcs.value.replace(/\./g, '').replace(',', '.'));
  const valorReaisMpollo = parseFloat(MetaMpolloReais.value.replace('R$', '').replace(/\./g, '').replace(',', '.'));
  SalvarMeta("PACO", valorPecasPaco, valorReaisPaco);
  SalvarMeta("M.POLLO", valorPecasMpollo, valorReaisMpollo);
});

//------------------------------------------------------------------------------------------------------------------------------------//


function preencherTabela(dados) {
  const tabela = document.getElementById('TabelaMetasSemanais');

  // Limpe o conteúdo existente na tabela
  tabela.innerHTML = '';

  // Crie uma linha de cabeçalho para os títulos das colunas
  const cabecalho = document.createElement('tr');

  // Adicione os títulos das colunas
  const titulos = ['Semana', '% PACO', 'PACO pçs', 'PACO R$', '% M.POLLO', 'M.POLLO pçs', 'M.POLLO R$'];
  for (let i = 0; i < titulos.length; i++) {
    const colunaTitulo = document.createElement('th');
    colunaTitulo.textContent = titulos[i];
    cabecalho.appendChild(colunaTitulo);
  }

  // Adicione a linha de cabeçalho à tabela
  tabela.appendChild(cabecalho);

  // Preencha a tabela com os dados
  const semanas = dados[0]['2- Detalhamento Semanal'];

  for (let i = 0; i < semanas.length; i++) {
    const linha = document.createElement('tr');

    // Adicione os dados de cada semana
    const semana = semanas[i];
    for (const chave in semana) {
      if (semana.hasOwnProperty(chave)) {
        const coluna = document.createElement('td');

        // Se a chave for '1- PACO %dist.' ou '2- M.POLLO %dist.', crie um input
        if (chave === '1- PACO %dist.' || chave === '2- M.POLLO %dist.') {
          const input = document.createElement('input');
          input.type = 'text';
          input.value = semana[chave]; // Preencha o input com o valor da API
          coluna.appendChild(input);
        } else {
          coluna.textContent = semana[chave];
        }

        

        linha.appendChild(coluna);
      }
    }

    // Adicione a linha à tabela
    tabela.appendChild(linha);
  }
};

async function CadastrarPercentual(Marca, Semana, Percentual) {
  const dados = {
    "plano": document.getElementById('InputPlanoMetasSemanais').value,
    "marca": Marca,
    "semana": Semana,
    "percentualDist": Percentual
}
console.log(dados)
  try {
      const response = await fetch(`http://192.168.0.183:8000/pcp/api/metaPlanoSemanal`, {
          method: 'POST',
          headers: {
              'Content-Type': 'application/json',
              'Authorization': 'a44pcp22'
          },
          body: JSON.stringify(dados),
      });

      if (response.ok) {
          const data = await response.json();
          console.log(data)
      } else {
          throw new Error("Erro na Atualização, Recarregue a página!\nSe o problema persistir contacte o Administrador!");
      }
  } catch (error) {
      console.error(error);
  }
}

 

document.getElementById("MetasSemanais").addEventListener("click", async function () {
  const Plano = document.getElementById("InputPlano");
  const PlanoSemana = document.getElementById("InputPlanoMetasSemanais");
  PlanoSemana.value = Plano.value;

  async function processInput(input, index) {
    const linha = input.closest("tr").rowIndex;
    const coluna = input.closest("td").cellIndex;
    const valor = parseFloat(input.value);

    if (coluna === 1) {
      await CadastrarPercentual('PACO', linha.toString(), valor);
    } else {
      await CadastrarPercentual('M.POLLO', linha.toString(), valor);
    }

    await ChamadaApi(ApiMetaSemanal, Plano.value);
    preencherTabela(ResultadoApi);

    const inputs = document.querySelectorAll("#TabelaMetasSemanais input");
    if (inputs[index + 1]) {
      inputs[index + 1].focus();
      inputs[index + 1].select();
    }

    inputs.forEach((input, index) => {
      input.addEventListener("keydown", async function(event) {
        if (event.key === "Enter") {
          event.preventDefault();
          await processInput(input, index);
    
          // Após processar a input, mova o foco para a próxima input (ou a anterior se necessário)
          const nextIndex = index + 1;
          if (nextIndex < inputs.length) {
            inputs[nextIndex].focus();
          }
        }
      });});
  }

  // Aguarde a conclusão da chamada da API usando await
  await ChamadaApi(ApiMetaSemanal, Plano.value);

  // Verifique se o resultado da API é uma mensagem de erro
  if (ResultadoApi[0].Mensagem === "O Plano nao tem intervalo planejado de inicio e fim") {
    // Exiba uma mensagem de erro ou faça o que for apropriado aqui
    alert("Para o cálculo da Meta Semanal é preciso que seja definido o intervalo de Vendas!");
  } else {
    // O resultado é válido, então você pode chamar a função preencherTabela
    document.getElementById("modalMetasSemanais").style.display = "block";
    preencherTabela(ResultadoApi);

    // Obtenha as inputs
    const inputs = document.querySelectorAll("#TabelaMetasSemanais input");

    // Adicione o ouvinte de evento "Enter" a todas as entradas
    inputs.forEach((input, index) => {
      input.addEventListener("keydown", async function(event) {
        if (event.key === "Enter") {
          event.preventDefault();
          await processInput(input, index);
    
          // Após processar a input, mova o foco para a próxima input (ou a anterior se necessário)
          const nextIndex = index + 1;
          if (nextIndex < inputs.length) {
            inputs[nextIndex].focus();
          }
        }
      });
    });

    
  }
})
