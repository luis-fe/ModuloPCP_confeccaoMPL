/* =====================================================
   CONFIGURAÇÕES GERAIS
===================================================== */
const API_BASE_URL = "http://10.162.0.53:9000/pcp/api";
const AUTH_HEADER = "a44pcp22";

let matriculaUsuario = null;

/* =====================================================
   INICIALIZAÇÃO DA TELA
===================================================== */
document.addEventListener("DOMContentLoaded", () => {

    matriculaUsuario = localStorage.getItem("matriculaUsuario");

    if (!matriculaUsuario) {
        alert("Usuário não autenticado");
        window.location.href = "TelaLogin.html";
        return;
    }

    console.log("Usuário logado:", matriculaUsuario);

    document.getElementById("btnSalvarMetas")
        .addEventListener("click", salvarMetasAno);

    document.getElementById("anoMeta")
        .addEventListener("change", carregarTudo);

    document.getElementById("tipoMeta")
        .addEventListener("change", carregarTudo);

    carregarTudo();
});

/* =====================================================
   CARREGAMENTO CENTRALIZADO
===================================================== */
function carregarTudo() {
    carregarMetas();
    carregarUltimaAtualizacao();
}

/* =====================================================
   BUSCAR METAS CADASTRADAS
===================================================== */
async function getMetasCadastradas(ano, empresa) {
    try {
        const response = await fetch(
            `${API_BASE_URL}/get_metas_cadastradas_ano_empresa?ano=${ano}&Empresa=${empresa}`,
            {
                headers: {
                    "Authorization": AUTH_HEADER
                }
            }
        );

        if (!response.ok) throw new Error();
        return await response.json();

    } catch (error) {
        console.error("Erro ao buscar metas:", error);
        return [];
    }
}

/* =====================================================
   ÚLTIMA ATUALIZAÇÃO DAS METAS
===================================================== */
async function carregarUltimaAtualizacao() {
    const ano = document.getElementById("anoMeta").value;
    const empresa = document.getElementById("tipoMeta").value;

    try {
        const response = await fetch(
            `${API_BASE_URL}/get_informacaoUltimaAlteMetas?ano=${ano}&Empresa=${empresa}`,
            {
                method: "GET",
                headers: {
                    "Authorization": AUTH_HEADER
                }
            }
        );

        if (!response.ok) throw new Error("Erro na API");

        const data = await response.json();

        // 🔎 Sem histórico
        if (!Array.isArray(data) || data.length === 0) {
            document.getElementById("dataAtualizacao").innerText = "—";
            document.getElementById("matriculaAtualizacao").innerText = "—";
            return;
        }

        const info = data[0];

        // ✅ API já retorna formatado
        document.getElementById("dataAtualizacao").innerText =
            info.dataHora || "—";

        document.getElementById("matriculaAtualizacao").innerText =
            info.matricula || "—";

        // 👉 Opcional (se quiser mostrar o nome)
        const nomeEl = document.getElementById("nomeAtualizacao");
        if (nomeEl) {
            nomeEl.innerText = info.nome || "—";
        }

    } catch (error) {
        console.error("Erro última atualização:", error);
        document.getElementById("dataAtualizacao").innerText = "Erro";
        document.getElementById("matriculaAtualizacao").innerText = "Erro";
    }
}


/* =====================================================
   CONSTANTES E FUNÇÕES AUXILIARES
===================================================== */
const meses = [
    "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
    "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
];

function formatarMoeda(valor) {
    return valor.toLocaleString("pt-BR", {
        style: "currency",
        currency: "BRL"
    });
}

function limparMoeda(valor) {
    return Number(
        valor.replace(/\./g, "")
             .replace(",", ".")
             .replace("R$", "")
             .trim()
    ) || 0;
}

function atualizarTotal() {
    let total = 0;
    document.querySelectorAll(".input-meta").forEach(input => {
        total += limparMoeda(input.value);
    });

    document.getElementById("totalGeral").innerText = formatarMoeda(total);
}

function formatarData(dataISO) {
    const data = new Date(dataISO);

    return data.toLocaleDateString("pt-BR") + " " +
        data.toLocaleTimeString("pt-BR", {
            hour: "2-digit",
            minute: "2-digit"
        });
}

/* =====================================================
   MONTAR TABELA DE METAS
===================================================== */
function montarTabela(valores = {}) {
    const tbody = document.getElementById("tabelaMetas");
    tbody.innerHTML = "";

    meses.forEach((mes, index) => {
        const valor = valores[index] || 0;

        tbody.innerHTML += `
            <tr>
                <td>${mes}</td>
                <td>
                    <input class="form-control input-meta"
                        value="${formatarMoeda(valor)}"
                        onfocus="this.select()"
                        oninput="atualizarTotal()"
                        onblur="this.value=formatarMoeda(limparMoeda(this.value));atualizarTotal()"
                        onkeydown="
                            if(event.key==='Enter'||event.key==='ArrowDown'){
                                event.preventDefault();
                                document.querySelectorAll('.input-meta')[${index + 1}]?.focus()
                            }
                            if(event.key==='ArrowUp'){
                                event.preventDefault();
                                document.querySelectorAll('.input-meta')[${index - 1}]?.focus()
                            }
                        ">
                </td>
            </tr>
        `;
    });

    atualizarTotal();
}

/* =====================================================
   CARREGAR METAS
===================================================== */
async function carregarMetas() {
    const ano = document.getElementById("anoMeta").value;
    const empresa = document.getElementById("tipoMeta").value;

    const dados = await getMetasCadastradas(ano, empresa);
    const valores = {};

    dados.forEach(item => {
        const mesIndex = parseInt(item.mes.split("-")[0], 10) - 1;
        valores[mesIndex] = limparMoeda(item.meta);
    });

    montarTabela(valores);
}

/* =====================================================
   SALVAR METAS DO ANO
===================================================== */
async function salvarMetasAno() {

    const empresa = document.getElementById("tipoMeta").value;
    const ano = document.getElementById("anoMeta").value;

    const mesesArray = [];
    const metasArray = [];

    document.querySelectorAll("#tabelaMetas tr").forEach(tr => {
        mesesArray.push(tr.children[0].innerText);
        metasArray.push(tr.querySelector("input").value);
    });

    const body = {
        Empresa: empresa,
        ano: ano,
        matricula: matriculaUsuario,
        meses: mesesArray,
        metas: metasArray
    };

    try {
        const response = await fetch(
            `${API_BASE_URL}/post_atualizarMetaMesesAno`,
            {
                method: "POST",
                headers: {
                    "Authorization": AUTH_HEADER,
                    "Content-Type": "application/json"
                },
                body: JSON.stringify(body)
            }
        );

        if (!response.ok) throw new Error();

        alert("✅ Metas salvas com sucesso!");
        carregarUltimaAtualizacao();

    } catch (error) {
        console.error(error);
        alert("❌ Erro ao salvar metas");
    }
}


/* =====================================================
   Voltar para o Dashboard
===================================================== */

document.getElementById('btnHome')?.addEventListener('click', () => {
    window.location.href = "/TelaFaturamentoMatriz.html";
});
