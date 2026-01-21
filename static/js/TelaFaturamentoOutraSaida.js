let DadosFaturamento = [];
const Token = 'a44pcp22';
let Retorna = '';
let FaturadoDia = '';
let Atualizacao = '';
let anoSelecionado = obterAnoAtualComoString();

// ------------------- SPINNER -------------------
function mostrarSpinner() {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = `
        <tr>
            <td colspan="6" style="text-align:center; padding:20px;">
                <div class="spinner-border text-light" role="status">
                    <span class="visually-hidden">Carregando...</span>
                </div>
            </td>
        </tr>
    `;
}

function ocultarSpinner() {
    document.getElementById('TabelaFaturamento').innerHTML = '';
}

// ---------------- FUNÇÃO PARA OBTER O ANO ATUAL ----------------
function obterAnoAtualComoString() {
    return new Date().getFullYear().toString();
}

// ------------------- FUNÇÃO PARA OBTER DADOS DA API -------------------
async function Faturamento() {
    try {
        mostrarSpinner();

        const response = await fetch(
            `http://10.162.0.53:9000/pcp/api/dashboarTV?ano=${anoSelecionado}&empresa=Outras`,
            {
                method: 'GET',
                headers: {
                    'Content-Type': 'application/json',
                    'Authorization': Token
                }
            }
        );

        if (!response.ok) throw new Error('Erro ao obter os dados da API');

        const data = await response.json();

        DadosFaturamento = data[0]['7- Detalhamento por Mes'] || [];
        Retorna = data[0]['3- No Retorna'] || '-';
        FaturadoDia = data[0]['4- No Dia'] || '-';
        Atualizacao = data[0]['6- Atualizado as'] || '-';

        ocultarSpinner();

    } catch (error) {
        console.error(error);
        DadosFaturamento = [];
        ocultarSpinner();
    }
}

// ------------------- TABELA PRINCIPAL -------------------
function criarTabelaEmbalagens(
    listaChamados,
    CondicaoFat,
    CondicaoMeta,
    condicao2,
    condicao3,
    vdimob
) {
    const TabelaFaturamento = document.getElementById('TabelaFaturamento');
    TabelaFaturamento.innerHTML = '';

    const cabecalho = TabelaFaturamento.createTHead();
    const linhaCabecalho = cabecalho.insertRow();

    [
        'Mês',
        'VD Mostruario',
        'Revenda MP.',
        'Devolucao MP',
        'VD Imobilizado',
        '_____Total____'
    ].forEach(texto => {
        const th = linhaCabecalho.insertCell();
        th.innerHTML = texto;
    });

    const corpoTabela = TabelaFaturamento.createTBody();

    if (!listaChamados || listaChamados.length === 0) {
        const linha = corpoTabela.insertRow();
        const celula = linha.insertCell();
        celula.colSpan = 6;
        celula.style.textAlign = 'center';
        celula.style.padding = '15px';
        celula.innerText = '-';

        document.getElementById('Retorna').textContent = '-';
        document.getElementById('FaturadoDia').textContent = '-';
        return;
    }

    listaChamados.forEach(item => {
        const linha = corpoTabela.insertRow();

        linha.insertCell(0).innerHTML = item.Mês ?? '-';
        linha.insertCell(1).innerHTML = item[CondicaoFat] ?? '-';
        linha.insertCell(2).innerHTML = item[CondicaoMeta] ?? '-';
        linha.insertCell(3).innerHTML = item[condicao2] ?? '-';
        linha.insertCell(4).innerHTML = item[vdimob] ?? '-';

        const total = linha.insertCell(5);
        total.innerHTML = item[condicao3] ?? '-';
        total.classList.add('cor-da-coluna');
    });

    document.getElementById('Retorna').textContent = `Retorna: ${Retorna}`;
    document.getElementById('FaturadoDia').textContent = `Faturado no Dia: ${FaturadoDia}`;
}

// ------------------- ATUALIZAR DASHBOARD -------------------
async function atualizarDashboard() {

    // 🔹 LIMPA A TABELA ANTES DE ATUALIZAR
    document.getElementById('TabelaFaturamento').innerHTML = '';

    // 🔹 (se tiver gráfico com Chart.js, destrua aqui também)
    if (window.meuGrafico) {
        window.meuGrafico.destroy();
        window.meuGrafico = null;
    }

    const statusText = document.getElementById('status');
    const valorStatus = statusText.innerText.trim().toUpperCase();

    await Faturamento();

    if (valorStatus === "MENSAL") {
        criarTabelaEmbalagens(
            DadosFaturamento,
            'Faturado',
            'meta',
            'Devolucao',
            'Total',
            'VD Imobilizado'
        );
        createBarChart('Faturado', 'meta');
    } else {
        criarTabelaEmbalagens(
            DadosFaturamento,
            'Fat.Acumulado',
            'meta acum.',
            'Devolucao',
            'Total',
            'VD Imobilizado'
        );
        createBarChart('Fat.Acumulado', 'meta acum.');
    }
}


// ------------------- EVENTOS -------------------
document.addEventListener('DOMContentLoaded', () => {

    // CSS criado uma única vez
    const style = document.createElement('style');
    style.innerHTML = `
        .cor-da-coluna {
            background-color: rgb(60, 160, 100);
            color: white;
            font-weight: bold;
        }
    `;
    document.head.appendChild(style);

    document.getElementById('box_ano').value = anoSelecionado;
    atualizarDashboard();

    document.getElementById('box_ano').addEventListener('change', e => {
        anoSelecionado = e.target.value;
        atualizarDashboard();
    });

    document.getElementById('Matriz')?.addEventListener('click', () => {
        window.location.href = "TelaFaturamentoMatriz.html";
    });
    document.getElementById('Geral')?.addEventListener('click', () => {
        window.location.href = "TelaFaturamentoGeral.html";
    });
    document.getElementById('Filial')?.addEventListener('click', () => {
        window.location.href = "TelaFaturamentoFilial.html";
    });
    document.getElementById('Outros')?.addEventListener('click', () => {
        window.location.href = "TelaFaturamentoOutraSaida.html";
    });

    document.getElementById('Config')?.addEventListener('click', () => {
        window.location.href = "login.html";
    });
});
