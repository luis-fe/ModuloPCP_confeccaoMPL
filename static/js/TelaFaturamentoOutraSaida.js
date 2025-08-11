let DadosFaturamento = '';
Token = 'a44pcp22';
let Retorna = ''
let FaturadoDia = ''
let Atualizacao = ''

async function Faturamento() {
    try {
        const response = await fetch(`http://192.168.0.183:8000/pcp/api/dashboarTV?ano=${2025}&empresa=${'Outras'}`, {
            method: 'GET',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
        });

        if (response.ok) {
            const data = await response.json();
            console.log(data);
            DadosFaturamento = data[0]['7- Detalhamento por Mes'];
            Retorna = data[0]['3- No Retorna'];
            FaturadoDia = data[0]['4- No Dia'];
            Atualizacao = data[0]['6- Atualizado as'];
            console.log(DadosFaturamento);
        } else {
            throw new Error('Erro ao obter os dados da API');
        }
    } catch (error) {
        console.error(error);
    }
}

function criarTabelaEmbalagens(listaChamados, CondicaoFat, CondicaoMeta, condicao2, condicao3, vdimob) {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = TabelaFaturamento.createTHead();
    const cabecalhoLinha = cabecalho.insertRow();

    const cabecalhoCelula1 = cabecalhoLinha.insertCell(0);
    cabecalhoCelula1.innerHTML = 'Mês';
    const cabecalhoCelula2 = cabecalhoLinha.insertCell(1);
    cabecalhoCelula2.innerHTML = 'VD Mostruario';
    const cabecalhoCelula3 = cabecalhoLinha.insertCell(2);
    cabecalhoCelula3.innerHTML = 'Revenda MP.';
    const cabecalhoCelula4 = cabecalhoLinha.insertCell(3);
    cabecalhoCelula4.innerHTML = 'Devolucao MP';
        const cabecalhoCelula5 = cabecalhoLinha.insertCell(4);
    cabecalhoCelula5.innerHTML = "VD Imobilizado";
    const cabecalhoCelula6 = cabecalhoLinha.insertCell(5);
    cabecalhoCelula6.innerHTML = "_____Total____";

    const corpoTabela = TabelaFaturamento.createTBody();

    listaChamados.forEach(item => {
        const linha = corpoTabela.insertRow();
        const celula1 = linha.insertCell(0);
        celula1.innerHTML = item.Mês;
        const celula2 = linha.insertCell(1);
        celula2.innerHTML = item[CondicaoFat];
        const celula3 = linha.insertCell(2);
        celula3.innerHTML = item[CondicaoMeta];
        const celula4 = linha.insertCell(3);
        celula4.innerHTML = item[condicao2];
        const celula5 = linha.insertCell(4);
        celula5.innerHTML = item[vdimob];
        const celula6 = linha.insertCell(5);
        celula6.innerHTML = item[condicao3];

        celula6.classList.add('cor-da-coluna');
        // Define as propriedades de estilo para a classe
const style = document.createElement('style');
style.innerHTML = `
  .cor-da-coluna {
    background-color: rgb(60, 160, 100);  // Substitua 'yellow' pela cor desejada
    // Adicione outras propriedades de estilo, se necessário
  }
`;
// Adiciona o elemento style ao cabeçalho do documento (pode ser o head ou outro local apropriado)
document.head.appendChild(style);


    });

    document.getElementById('Retorna').textContent = `Retorna: ${Retorna}`
    document.getElementById('FaturadoDia').textContent = `Faturado no Dia: ${FaturadoDia}`
    

}



document.getElementById('Matriz').addEventListener('click', () => {
    window.location.href = "TelaFaturamentoMatriz.html";
})

document.getElementById('Geral').addEventListener('click', () => {
    window.location.href = "TelaFaturamentoGeral.html";
})

document.getElementById('Filial').addEventListener('click', () => {
    window.location.href = "TelaFaturamentoFilial.html";
})
document.getElementById('Varejo').addEventListener('click', () => {
    window.location.href = "TelaFaturamentoVarejo.html";
})
document.getElementById('Outros').addEventListener('click', () => {
    window.location.href = "TelaFaturamentoOutraSaidas.html";
})

