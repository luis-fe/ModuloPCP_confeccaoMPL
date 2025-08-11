let DadosFaturamento = '';
Token = 'a44pcp22';
let Retorna = ''
let FaturadoDia = ''

async function Faturamento() {
    try {
        const response = await fetch(`http://192.168.0.183:8000/pcp/api/dashboarTV?ano=${2025}`, {
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
            console.log(DadosFaturamento);
        } else {
            throw new Error('Erro ao obter os dados da API');
        }
    } catch (error) {
        console.error(error);
    }
}


let meuGrafico;
async function createBarChart(CondicaoFat, CondicaoMeta) {
    const meses = DadosFaturamento.map(item => item['Mês']);
    const dadosFiltrados = DadosFaturamento.filter(item => item.Mês !== '✈TOTAL');
    const valoresFaturadosMilhoes = dadosFiltrados.map((item) => {
        const faturado = item[CondicaoFat].replace('R$', '').replace(/\./g, '').replace(',', '.');
        return parseFloat(faturado) ;
    });
    const MetasMilhoes = dadosFiltrados.map((item) => {
        const meta = item[CondicaoMeta].replace('R$', '').replace(/\./g, '').replace(',', '.');
        return parseFloat(meta) ;
    });
       
    console.log(valoresFaturadosMilhoes)
    console.log(MetasMilhoes)

    const ctx = document.getElementById('meuGraficoDeBarras').getContext('2d');

    if (meuGrafico) {
        meuGrafico.destroy();
    }
    meuGrafico = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: meses,
            datasets: [{
                label: 'Faturamento por Mês',
                data: valoresFaturadosMilhoes,
                backgroundColor: 'rgb(17, 45, 126)',
                borderColor: 'rgb(255, 255, 255)',
                borderWidth: 1,
            },
            {
                type: 'bar',
                label: 'Meta',
                data: MetasMilhoes,
                backgroundColor: 'rgb(241, 129, 17)',
                borderColor: 'rgb(255, 255, 255)',
                borderWidth: 1,
            },
        ],
        },
        options: {
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function (value) {
                            return 'R$ ' + value.toLocaleString('pt-BR');
                        },
                    },
                },
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            const value = context.parsed.y;
                            return 'R$ ' + value.toLocaleString('pt-BR');
                        },
                    },
                },
            },
        },
    });
}



function criarTabelaEmbalagens(listaChamados, CondicaoFat, CondicaoMeta) {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = ''; // Limpa o conteúdo da tabela antes de preenchê-la novamente

    // Cria o cabeçalho da tabela
    const cabecalho = TabelaFaturamento.createTHead();
    const cabecalhoLinha = cabecalho.insertRow();

    const cabecalhoCelula1 = cabecalhoLinha.insertCell(0);
    cabecalhoCelula1.innerHTML = 'Mês';
    const cabecalhoCelula2 = cabecalhoLinha.insertCell(1);
    cabecalhoCelula2.innerHTML = 'Meta';
    const cabecalhoCelula3 = cabecalhoLinha.insertCell(2);
    cabecalhoCelula3.innerHTML = 'Faturado';

    const corpoTabela = TabelaFaturamento.createTBody();

    listaChamados.forEach(item => {
        const linha = corpoTabela.insertRow();
        const celula1 = linha.insertCell(0);
        celula1.innerHTML = item.Mês;
        const celula2 = linha.insertCell(1);
        celula2.innerHTML = item[CondicaoMeta];
        const celula3 = linha.insertCell(2);
        celula3.innerHTML = item[CondicaoFat];
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


