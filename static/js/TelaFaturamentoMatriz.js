// ------------------- VARIÁVEIS GLOBAIS -------------------
// Variáveis que armazenam dados da API, gráficos e ano selecionado
let DadosFaturamento = '';
let Token = 'a44pcp22';
let Retorna = '';
let FaturadoDia = '';
let Atualizacao = '';
let meuGrafico;
let anoSelecionado = obterAnoAtualComoString(); // Obtem o ano atual como string

// ------------------- SPINNER -------------------
// Mostra animação de carregamento na tabela
function mostrarSpinner() {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = `
        <tr><td colspan="4" style="text-align:center; padding:20px;">
            <div class="spinner-border text-light" role="status">
                <span class="visually-hidden">Carregando...</span>
            </div>
        </td></tr>
    `;
}

// Oculta o spinner, limpando a tabela
function ocultarSpinner() {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = '';
}

// ------------------- FUNÇÃO PARA OBTER DADOS DA API -------------------
// Busca dados da API e armazena nas variáveis globais

async function Faturamento() {
    try {
        mostrarSpinner();

        const response = await fetch(`http://192.168.0.183:8000/pcp/api/dashboarTV?ano=${anoSelecionado}&empresa=1`, {
            method: 'GET',
            headers: { 'Content-Type': 'application/json', 'Authorization': Token },
        });

        if (!response.ok) throw new Error('Erro ao obter os dados da API');

        const data = await response.json();
        DadosFaturamento = data[0]['7- Detalhamento por Mes']; // Detalhamento mensal
        Retorna = data[0]['3- No Retorna'];                   // Valor do Retorna
        FaturadoDia = data[0]['4- No Dia'];                  // Valor do Faturado no Dia
        Atualizacao = data[0]['6- Atualizado as'];           // Última atualização
        console.log(DadosFaturamento);

        ocultarSpinner();
    } catch (error) {
        console.error(error);
        ocultarSpinner();
        DadosFaturamento = []; // Se houver erro, garante que a tabela fique vazia
    }
}

// ---------------- FUNÇÃO PARA OBTER O ANO ATUAL -------------------------
// Retorna o ano atual como string
function obterAnoAtualComoString() {
    const anoAtual = new Date().getFullYear();
    return anoAtual.toString();
}

// ------------------- GRÁFICO DE BARRAS -------------------
// Cria o gráfico de barras do faturamento vs meta

async function createBarChart(CondicaoFat, CondicaoMeta) {
    const meses = DadosFaturamento.map(item => item['Mês']);
    const dadosFiltrados = DadosFaturamento.filter(item => item.Mês !== '✈TOTAL'); // Exclui linha total

    const valoresFaturadosMilhoes = dadosFiltrados.map(item =>
        parseFloat(item[CondicaoFat].replace('R$', '').replace(/\./g, '').replace(',', '.'))
    );
    const MetasMilhoes = dadosFiltrados.map(item =>
        parseFloat(item[CondicaoMeta].replace('R$', '').replace(/\./g, '').replace(',', '.'))
    );

    const ctx = document.getElementById('meuGraficoDeBarras').getContext('2d');
    if (meuGrafico) meuGrafico.destroy(); // Remove gráfico anterior

    meuGrafico = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: meses,
            datasets: [
                {
                    label: 'Faturamento por Mês',
                    data: valoresFaturadosMilhoes,
                    backgroundColor: 'rgb(13, 110, 253)',
                    borderColor: 'rgb(211, 211, 211)',
                    borderWidth: 1
                },
                {
                    type: 'bar',
                    label: 'Meta',
                    data: MetasMilhoes,
                    backgroundColor: 'rgba(226, 233, 236, 1)',
                    borderColor: 'rgb(211, 211, 211)',
                    borderWidth: 1
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: value => value >= 1000000
                            ? 'R$' + (value / 1000000) + 'M'
                            : 'R$' + value.toLocaleString('pt-BR'),
                        font: { size: 15 }
                    }
                },
                x: {
                    barPercentage: 0.1,
                    ticks: { font: { size: 15 } }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: { font: { size: 15, weight: 'bold' } }
                },
                tooltip: {
                    callbacks: {
                        label: context => 'R$' + context.parsed.y.toLocaleString('pt-BR')
                    }
                }
            }
        }
    });
}

// ------------------- TABELA PRINCIPAL -------------------
// Cria tabela de faturamento, meta e % atingido
function criarTabelaEmbalagens(listaChamados, CondicaoFat, CondicaoMeta) {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = '';

    // Cabeçalho da tabela
    const cabecalho = TabelaFaturamento.createTHead();
    const cabecalhoLinha = cabecalho.insertRow();
    const headers = ['Mês', 'Meta', 'Faturado', '% Atingido'];
    
    headers.forEach(text => {
        const th = cabecalhoLinha.insertCell();
        th.innerHTML = text;
        th.style.backgroundColor = '#0d6efd';
        th.style.color = '#fff';
        th.style.fontWeight = 'bold';
    });

    const corpoTabela = TabelaFaturamento.createTBody();

    // Caso não haja dados, exibe "-" na tabela
    if (!listaChamados || listaChamados.length === 0) {
        const linha = corpoTabela.insertRow();
        const celula = linha.insertCell();
        celula.colSpan = 4;
        celula.style.textAlign = 'center';
        celula.style.padding = '15px';
        celula.innerText = '-';
        document.getElementById('Retorna').textContent = '-';
        document.getElementById('FaturadoDia').textContent = '-';
        return; // encerra a função
    }

    // Preenchimento das linhas com dados
    listaChamados.forEach(item => {
        const linha = corpoTabela.insertRow();
        const celula1 = linha.insertCell(0);
        celula1.style.fontSize = '1rem';
        celula1.innerHTML = item.Mês;
        celula1.classList.add('mes');
        celula1.style.cursor = 'pointer';
        celula1.addEventListener('click', () => abrirModal(item.Mês));

        const celula2 = linha.insertCell(1);
        celula2.style.fontSize = '1rem';
        celula2.innerHTML = item[CondicaoMeta];

        const celula3 = linha.insertCell(2);
        celula3.style.fontSize = '1rem';
        celula3.innerHTML = item[CondicaoFat];

        const meta = parseFloat(item[CondicaoMeta].replace('R$', '').replace(/\./g, '').replace(',', '.')) || 0;
        const faturado = parseFloat(item[CondicaoFat].replace('R$', '').replace(/\./g, '').replace(',', '.')) || 0;
        const percentual = meta > 0 ? (faturado / meta) * 100 : 0;

        linha.insertCell(3).innerHTML = percentual.toFixed(2) + '%';
    });

    // Atualiza informações adicionais
    document.getElementById('Retorna').textContent = `Retorna: ${Retorna}`;
    document.getElementById('FaturadoDia').textContent = `Faturado no Dia: ${FaturadoDia}`;
}
/*
// ------------------- MODAL -------------------
// Elementos do modal
const modal = document.getElementById('modal');
const modalContent = document.getElementById('modal-content');
const modalBody = document.getElementById('modal-body');
const closeModalBtn = document.getElementById('close-modal');
const modalHeader = document.getElementById('modal-header');

// Gera dados fake para exemplo no modal
function gerarDadosFake(mes) {
    const dados = [];
    const ano = 2025;
    const meses = {
        "Janeiro": 0, "Fevereiro": 1, "Março": 2, "Abril": 3,
        "Maio": 4, "Junho": 5, "Julho": 6, "Agosto": 7,
        "Setembro": 8, "Outubro": 9, "Novembro": 10, "Dezembro": 11
    };
    const mesNumero = meses[mes];

    for (let i = 1; i <= 21; i++) {
        const dia = Math.floor(Math.random() * 28) + 1;
        const data = new Date(ano, mesNumero, dia);
        const dataFormatada = data.toLocaleDateString('pt-BR');
        const valor = Math.floor(Math.random() * 500) + 50;
        dados.push({ data: dataFormatada, valor });
    }

    // Ordena dados por data
    dados.sort((a, b) => new Date(a.data.split('/').reverse().join('-')) - new Date(b.data.split('/').reverse().join('-')));
    return dados;
}

// Abre modal com dados detalhados
function abrirModal(mes) {
    let dados = gerarDadosFake(mes);
    let sortDirection = { data: 'asc', valor: 'asc' };

    function renderTable() {
        let tabelaHTML = `<h5>Faturamento de ${mes}</h5>
            <table>
                <tr>
                    <th id="th-data" style="cursor:pointer">Data &#9650;</th>
                    <th id="th-valor" style="cursor:pointer">Valor (R$) &#9650;</th>
                </tr>`;

        dados.forEach(d => {
            tabelaHTML += `<tr>
                <td>${d.data}</td>
                <td>${d.valor}</td>
            </tr>`;
        });

        tabelaHTML += `</table>`;
        modalBody.innerHTML = tabelaHTML;

  // Ordenação por Data
        document.getElementById('th-data').onclick = () => {
            if (sortDirection.data === 'asc') {
                dados.sort((a, b) => new Date(a.data.split('/').reverse().join('-')) - new Date(b.data.split('/').reverse().join('-')));
                sortDirection.data = 'desc';
                document.getElementById('th-data').innerHTML = 'Data &#9660;';
            } else {
                dados.sort((a, b) => new Date(b.data.split('/').reverse().join('-')) - new Date(a.data.split('/').reverse().join('-')));
                sortDirection.data = 'asc';
                document.getElementById('th-data').innerHTML = 'Data &#9650;';
            }
            renderTable();
        };

        // Ordenação por Valor
        document.getElementById('th-valor').onclick = () => {
            if (sortDirection.valor === 'asc') {
                dados.sort((a, b) => a.valor - b.valor);
                sortDirection.valor = 'desc';
                document.getElementById('th-valor').innerHTML = 'Valor (R$) &#9660;';
            } else {
                dados.sort((a, b) => b.valor - a.valor);
                sortDirection.valor = 'asc';
                document.getElementById('th-valor').innerHTML = 'Valor (R$) &#9650;';
            }
            renderTable();
        };
    }

    renderTable();
    modal.style.display = 'block';
}

// Fecha modal
closeModalBtn.onclick = () => modal.style.display = 'none';
window.onclick = (e) => { if (e.target === modal) modal.style.display = 'none'; };

// Movimentar modal (drag)
modalHeader.onmousedown = function(e) {
    e.preventDefault();
    const rect = modalContent.getBoundingClientRect();
    let offsetX = e.clientX - rect.left;
    let offsetY = e.clientY - rect.top;

    function onMouseMove(e) {
        let left = e.clientX - offsetX;
        let top = e.clientY - offsetY;
        left = Math.max(0, Math.min(window.innerWidth - modalContent.offsetWidth, left));
        top = Math.max(0, Math.min(window.innerHeight - modalContent.offsetHeight, top));
        modalContent.style.left = left + 'px';
        modalContent.style.top = top + 'px';
        modalContent.style.transform = 'none';
    }

    document.addEventListener('mousemove', onMouseMove);
    document.onmouseup = () => document.removeEventListener('mousemove', onMouseMove);
};

// Redimensionar modal
let isResizing = false;
let resizeDir = '';

modalContent.addEventListener('mousemove', (e) => {
    const rect = modalContent.getBoundingClientRect();
    const edge = 8;
    resizeDir = '';
    if (e.clientX < rect.left + edge && e.clientY < rect.top + edge) resizeDir = 'nw';
    else if (e.clientX > rect.right - edge && e.clientY < rect.top + edge) resizeDir = 'ne';
    else if (e.clientX < rect.left + edge && e.clientY > rect.bottom - edge) resizeDir = 'sw';
    else if (e.clientX > rect.right - edge && e.clientY > rect.bottom - edge) resizeDir = 'se';
    else if (e.clientX < rect.left + edge) resizeDir = 'w';
    else if (e.clientX > rect.right - edge) resizeDir = 'e';
    else if (e.clientY < rect.top + edge) resizeDir = 'n';
    else if (e.clientY > rect.bottom - edge) resizeDir = 's';

    modalContent.classList.remove(
        'resizing-e','resizing-s','resizing-se','resizing-n','resizing-w','resizing-nw','resizing-ne','resizing-sw'
    );
    if (resizeDir) modalContent.classList.add('resizing-' + resizeDir);
});

modalContent.addEventListener('mousedown', (e) => {
    if (!resizeDir) return;
    e.preventDefault();
    isResizing = true;
    const startX = e.clientX;
    const startY = e.clientY;
    const startWidth = modalContent.offsetWidth;
    const startHeight = modalContent.offsetHeight;
    const startLeft = modalContent.offsetLeft;
    const startTop = modalContent.offsetTop;

    function onMouseMove(e) {
        let newWidth = startWidth;
        let newHeight = startHeight;
        let newLeft = startLeft;
        let newTop = startTop;

        if (resizeDir.includes('e')) newWidth = startWidth + (e.clientX - startX);
        if (resizeDir.includes('s')) newHeight = startHeight + (e.clientY - startY);
        if (resizeDir.includes('w')) { newWidth = startWidth - (e.clientX - startX); newLeft = startLeft + (e.clientX - startX); }
        if (resizeDir.includes('n')) { newHeight = startHeight - (e.clientY - startY); newTop = startTop + (e.clientY - startY); }

        newWidth = Math.max(200, Math.min(window.innerWidth - newLeft, newWidth));
        newHeight = Math.max(150, Math.min(window.innerHeight - newTop, newHeight));
        newLeft = Math.max(0, Math.min(newLeft, window.innerWidth - newWidth));
        newTop = Math.max(0, Math.min(newTop, window.innerHeight - newHeight));

        modalContent.style.width = newWidth + 'px';
        modalContent.style.height = newHeight + 'px';
        modalContent.style.left = newLeft + 'px';
        modalContent.style.top = newTop + 'px';
        modalContent.style.transform = 'none';
    }

    function onMouseUp() {
        isResizing = false;
        document.removeEventListener('mousemove', onMouseMove);
        document.removeEventListener('mouseup', onMouseUp);
    }

    document.addEventListener('mousemove', onMouseMove);
    document.addEventListener('mouseup', onMouseUp);
});*/

// ------------------- NAVEGAÇÃO ENTRE TELAS -------------------
document.getElementById('Matriz').addEventListener('click', () => { window.location.href = "TelaFaturamentoMatriz.html"; });
document.getElementById('Geral').addEventListener('click', () => { window.location.href = "TelaFaturamentoGeral.html"; });
document.getElementById('Filial').addEventListener('click', () => { window.location.href = "TelaFaturamentoFilial.html"; });
document.getElementById('Outros').addEventListener('click', () => { window.location.href = "TelaFaturamentoOutraSaidas.html"; });

// ------------------- ATUALIZAR DASHBOARD -------------------
// Atualiza tabela e gráfico conforme status e ano selecionado
async function atualizarDashboard() {
    const statusText = document.getElementById('status');
    const valorStatus = statusText.innerText.trim().toUpperCase();

    await Faturamento(); // Obtem dados da API

    if (valorStatus === "MENSAL") {
        criarTabelaEmbalagens(DadosFaturamento, 'Faturado', 'meta');
        createBarChart('Faturado', 'meta');
    } else {
        criarTabelaEmbalagens(DadosFaturamento, 'Fat.Acumulado', 'meta acum.');
        createBarChart('Fat.Acumulado', 'meta acum.');
    }
}

// ------------------- SELECT DE ANO -------------------
// Quando seleciona um ano, atualiza dashboard
document.getElementById('selectAno').addEventListener('change', (e) => {
    anoSelecionado = e.target.value;
    atualizarDashboard();
});

// ------------------- AO CARREGAR A PÁGINA -------------------
document.addEventListener('DOMContentLoaded', () => {
    const selectAno = document.getElementById('selectAno');
    selectAno.value = anoSelecionado; // marca o ano atual
    atualizarDashboard();             // atualiza dashboard
});



document.getElementById('Config').addEventListener('click', () => {
    window.location.href = "login.html";
});
