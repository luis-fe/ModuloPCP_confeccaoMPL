const tabela = document.getElementById("tabelaNotas");
const btnAdicionar = document.getElementById("btnAdicionar");
const listaNotas = document.getElementById("listaNotas");
const btnSalvar = document.getElementById("btnSalvar");

let linhaSelecionada = null;
let tiposNotasAPI = [];

/* ============================
   ADICIONAR NOVA LINHA
============================ */
btnAdicionar.addEventListener("click", () => {
    const tr = document.createElement("tr");

    tr.innerHTML = `
        <td class="text-start">
            <button class="btn btn-sm btn-outline-success me-1 btn-pesquisar" title="Pesquisar tipo de nota">
                🔍
            </button>

            <button class="btn btn-sm btn-outline-danger me-2 btn-excluir" title="Excluir linha">
                🗑️
            </button>

            <span class="tipo-nota text-white fw-bold"></span>
        </td>

        <td>
            <select class="form-select form-select-sm text-center">
                <option value="true">Sim</option>
                <option value="false">Não</option>
            </select>
        </td>
    `;

    tr.querySelector(".btn-pesquisar").addEventListener("click", () => {
        abrirModalNotas(tr);
    });

    tr.querySelector(".btn-excluir").addEventListener("click", () => {
        if (confirm("Deseja realmente excluir esta linha?")) {
            tr.remove();
        }
    });

    tabela.appendChild(tr);
});

/* ============================
   ABRIR MODAL
============================ */
async function abrirModalNotas(tr) {
    linhaSelecionada = tr;

    if (tiposNotasAPI.length === 0) {
        await carregarTiposNotas();
    }

    renderizarLista();

    const modal = new bootstrap.Modal(
        document.getElementById("modalNotas")
    );
    modal.show();
}

/* ============================
   BUSCAR TIPOS DE NOTAS (API)
============================ */
async function carregarTiposNotas() {
    try {
        const response = await fetch(
            "http://10.162.0.53:9000/pcp/api/TipoNotasCsw",
            {
                method: "GET",
                headers: {
                    "Authorization": "a44pcp22"
                }
            }
        );

        if (!response.ok) {
            throw new Error("Erro ao buscar tipos de notas");
        }

        tiposNotasAPI = await response.json();

    } catch (error) {
        alert("Erro ao carregar tipos de notas");
        console.error(error);
    }
}

/* ============================
   RENDERIZAR LISTA DO MODAL
============================ */
function renderizarLista() {
    listaNotas.innerHTML = "";

    tiposNotasAPI.forEach(item => {
        const li = document.createElement("li");
        li.className =
            "list-group-item list-group-item-action bg-dark text-white";
        li.style.cursor = "pointer";

        li.textContent = `${item.codigo} - ${item.descricao}`;

        li.addEventListener("click", () => {
            selecionarNota(item);
        });

        listaNotas.appendChild(li);
    });
}

/* ============================
   SELECIONAR NOTA
============================ */
function selecionarNota(item) {
    if (!linhaSelecionada) return;

    const span = linhaSelecionada.querySelector(".tipo-nota");
    span.textContent = `${item.codigo} - ${item.descricao}`;

    const modal = bootstrap.Modal.getInstance(
        document.getElementById("modalNotas")
    );
    modal.hide();
}

/* ============================
   SALVAR CONFIGURAÇÕES
============================ */
btnSalvar.addEventListener("click", salvarConfiguracoes);

async function salvarConfiguracoes() {
    const linhas = document.querySelectorAll("#tabelaNotas tr");

    if (linhas.length === 0) {
        alert("Nenhuma configuração para salvar.");
        return;
    }

    const tipoNotas = [];
    const consideraTotalizador = [];

    linhas.forEach(linha => {
        const tipoNotaTexto =
            linha.querySelector(".tipo-nota")?.textContent.trim();

        const considera =
            linha.querySelector("select")?.value;

        if (tipoNotaTexto) {
            tipoNotas.push(tipoNotaTexto);
            consideraTotalizador.push(considera);
        }
    });

    const payload = {
        Empresa: "Matriz",
        tipoNotas,
        consideraTotalizador
    };

    await enviarParaAPI(payload);
}

/* ============================
   POST NA API
============================ */
async function enviarParaAPI(dados) {
    try {
        const response = await fetch(
            "http://10.162.0.53:9000/pcp/api/post_inserirConfiguracoes",
            {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                    "Authorization": "a44pcp22"
                },
                body: JSON.stringify(dados)
            }
        );

        if (!response.ok) {
            throw new Error("Erro ao salvar configurações");
        }

        alert("Configurações salvas com sucesso ✅");

    } catch (error) {
        console.error(error);
        alert("Erro ao salvar configurações ❌");
    }
}

/* ============================
   CARREGAR CONFIGURAÇÕES SALVAS
============================ */
document.addEventListener("DOMContentLoaded", () => {
    carregarConfiguracoesSalvas();
});

async function carregarConfiguracoesSalvas() {
    try {
        const empresa = "Matriz";

        const response = await fetch(
            `http://10.162.0.53:9000/pcp/api/get_informacaoNotasConfigruadas?Empresa=${empresa}`,
            {
                method: "GET",
                headers: {
                    "Authorization": "a44pcp22"
                }
            }
        );

        if (!response.ok) {
            throw new Error("Erro ao buscar configurações");
        }

        const dados = await response.json();
        tabela.innerHTML = "";

        dados.forEach(item => {
            adicionarLinhaConfigurada(item);
        });

    } catch (error) {
        console.error(error);
        alert("Erro ao carregar configurações salvas");
    }
}

/* ============================
   ADICIONAR LINHA DA API
============================ */
function adicionarLinhaConfigurada(item) {
    const tr = document.createElement("tr");

    tr.innerHTML = `
        <td class="text-start">
            <button class="btn btn-sm btn-outline-success me-1 btn-pesquisar">
                🔍
            </button>

            <button class="btn btn-sm btn-outline-danger me-2 btn-excluir">
                🗑️
            </button>

            <span class="tipo-nota text-white fw-bold">
                ${item.tipoNota}
            </span>
        </td>

        <td>
            <select class="form-select form-select-sm text-center">
                <option value="true">Sim</option>
                <option value="false">Não</option>
            </select>
        </td>
    `;

    tr.querySelector(".btn-pesquisar").addEventListener("click", () => {
        abrirModalNotas(tr);
    });

    tr.querySelector(".btn-excluir").addEventListener("click", () => {
        if (confirm("Deseja realmente excluir esta linha?")) {
            tr.remove();
        }
    });

    tr.querySelector("select").value =
        String(item.consideraTotalizador);

    tabela.appendChild(tr);
}

/* =====================================================
   Voltar para o Dashboard
===================================================== */

document.getElementById('btnHome')?.addEventListener('click', () => {
    window.location.href = "/TelaFaturamentoMatriz.html";
});
