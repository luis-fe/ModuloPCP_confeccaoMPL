const ApiConsultaCodigos = "http://192.168.0.183:8000/pcp/api/Plano";
let PlanoSelecionado;
let CriarTabela = 0;

function criarTabelaDistribuicao(listaPlanos) {
    const tabelaPlano = document.getElementById('TabelaPlanos');
    tabelaPlano.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckbox = document.createElement('th');
    const ColunaCodigo = document.createElement('th');
    const ColunaDescricao = document.createElement('th');

    colunaCheckbox.textContent = '';
    ColunaCodigo.textContent = 'Código';
    ColunaDescricao.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckbox);
    cabecalhoRow.appendChild(ColunaCodigo);
    cabecalhoRow.appendChild(ColunaDescricao);
    cabecalho.appendChild(cabecalhoRow);
    tabelaPlano.appendChild(cabecalho);

    listaPlanos.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckbox = document.createElement('td');
        const ColunaCodigo = document.createElement('td');
        const ColunaDescricao = document.createElement('td');

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = item["01- Codigo Plano"];
        checkbox.name = 'checkboxPlano';
        checkbox.addEventListener('change', function (event) {
            const checkboxes = document.getElementsByName('checkboxPlano');
            checkboxes.forEach(box => {
                if (box !== event.target) {
                    box.checked = false;
                }
            });
        });
        colunaCheckbox.appendChild(checkbox);
        ColunaCodigo.textContent = item["01- Codigo Plano"];
        ColunaDescricao.textContent = item["02- Descricao do Plano"];

        row.appendChild(colunaCheckbox);
        row.appendChild(ColunaCodigo);
        row.appendChild(ColunaDescricao);

        tabelaPlano.appendChild(row);
    });
}

function ConsultaCodigoPlano() {
    fetch(ApiConsultaCodigos, {
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
                throw new Error('Erro ao obter a lista de usuários');
            }
        })
        .then(data => {
            criarTabelaDistribuicao(data);
        })
        .catch(error => {
            console.error(error);
        });
}

const BotaoPesquisa = document.getElementById('BotaoPesquisar');
const modalPlano = document.getElementById('modalPlanos');
const fecharModalPlanos = document.getElementById('fecharModalPlanos');

BotaoPesquisa.addEventListener('click', function () {
    ConsultaCodigoPlano();
    modalPlano.style.display = 'block';
});

fecharModalPlanos.addEventListener('click', function () {
    modalPlano.style.display = 'none';
});

document.getElementById('fecharModalConfigVendas').addEventListener('click', function () {
    document.getElementById('modalConfigVendas').style.display = 'none';
});

document.getElementById('fecharModalMetasSemanais').addEventListener('click', function () {
    document.getElementById('modalMetasSemanais').style.display = 'none';
});


//------------------------------------------------Consultando e Adicionando Plano Existente--------------------------------------------//
function formatDate(dateStr) {
    const parts = dateStr.split('/');
    const day = parts[0];
    const month = parts[1];
    const year = parts[2];

    return `${year}-${month}-${day}`;
}

function formatDateParametro(dateStr) {
    const parts = dateStr.split('-'); // Modificamos o separador para '-'
    const year = parts[0];
    const month = parts[1];
    const day = parts[2];

    return `${day}/${month}/${year}`; // Formato "dd/MM/yyyy"
}



const InputCodigo = document.getElementById('InputPlano')
const InputDescricao = document.getElementById('InputDescricao')
const InputDataInicialVendas = document.getElementById('InputDatainicio')
const InputDataFinallVendas = document.getElementById('InputDataFim')
const InputDataInicialFat = document.getElementById('InputDatainicioFaturamento')
const InputDataFinallFat = document.getElementById('InputDataFimFaturamento')
const AbrirColecoes = document.getElementsByClassName("CampoTabelas")[0];
const ButtonCriar = document.getElementById('ButtonCriarPlano');
const ButtonEditar = document.getElementById('ButtonEditarPlano');
const ButtonEditarAbc = document.getElementById('ButtonEditarCurvaAbc');
const ButtonCurvaVenda = document.getElementById('ButtonEditarCurvaDeVendas');

function carregarDadosFromCodigo() {
    const valorInput = InputCodigo.value.trim();

    if (valorInput === '') {
        alert('O campo está vazio. Por favor, insira um valor.');
        InputCodigo.focus();
        document.getElementById('InputDescricao').value = "";
        document.getElementById('InputDatainicio').value = "";
        document.getElementById('InputDataFim').value = "";
        document.getElementById('InputDatainicioFaturamento').value = "";
        document.getElementById('InputDataFimFaturamento').value = "";
        AbrirColecoes.style.display = "none";
        ButtonCriar.style.display = "none";
        ButtonEditar.style.display = "none";
        ButtonEditarAbc.style.display = "none";
        ButtonCurvaVenda.style.display = "none"

    } else {
        console.log(valorInput)

        fetch(`http://192.168.0.183:8000/pcp/api/StatusPlano/${valorInput}`, {
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
                throw new Error('Erro ao obter a lista de usuários');
            }
        })
        .then(data => {
            if (data["001 status"] === false) {
                document.getElementById('InputDescricao').value = "";
                InputDescricao.focus();
                document.getElementById('InputDatainicio').value = "";
                document.getElementById('InputDataFim').value = "";
                document.getElementById('InputDatainicioFaturamento').value = "";
                document.getElementById('InputDataFimFaturamento').value = "";
                ButtonEditar.style.display = "none";
                ButtonEditarAbc.style.display = "none";
                ButtonCurvaVenda.style.display = "none";
                ButtonCriar.style.display = "flex";
                AbrirColecoes.style.display = "none";
            } else {
                document.getElementById('InputDescricao').value = data["02- Descricao do Plano"];
                document.getElementById('InputDatainicio').value = formatDate(data["03- Inicio Venda"]);
                document.getElementById('InputDataFim').value = formatDate(data["04- Final Venda"]);
                document.getElementById('InputDatainicioFaturamento').value = formatDate(data["05- Inicio Faturamento"]);
                document.getElementById('InputDataFimFaturamento').value = formatDate(data["06- Final Faturamento"]);
                ButtonEditar.style.display = 'flex';
                ButtonCriar.style.display = "none";
                ButtonEditarAbc.style.display = "flex";
                ButtonEditarAbc.style.marginLeft = "10px";
                ButtonCurvaVenda.style.display = "flex";
                ButtonCurvaVenda.style.marginLeft = "10px";
                ButtonCurvaVenda.style.width = "90px";
                AbrirColecoes.style.display = "flex";
                ConsultaColecoesVinculadas();
                ConsultaTiposDeNotasVinculadas();
                ConsultaLotesVinculados();
            }
        })
        .catch(error => {
            console.error(error);
        });
    }
}

InputCodigo.addEventListener('keydown', function (event) {
    if (event.key === 'Enter') {
        carregarDadosFromCodigo();
        event.preventDefault();
    }
});

//---------------------------------------------------------------------- CRIANDO NOVO PLANO-------------------------------------------------------------------------------//
const BotaoCriarPlano = document.getElementById("ButtonCriarPlano")

BotaoCriarPlano.addEventListener('click', function () {

    if (InputCodigo.value === "" && InputDescricao.value === "") {
        InputDescricao.style.borderColor = "red";
        InputDescricao.placeholder = "Campo Obrigatório"
        InputCodigo.style.borderColor = "red";
        InputCodigo.placeholder = "Obrigatório"
        setTimeout(function () {
            InputCodigo.style.borderColor = "darkgray";
            InputCodigo.placeholder = ""
            InputDescricao.style.borderColor = "darkgray";
            InputDescricao.placeholder = ""
        }, 5000);
        return;
    };

    if (InputCodigo.value === "") {
        InputCodigo.style.borderColor = "red";
        InputCodigo.placeholder = "Obrigatório"
        setTimeout(function () {
            InputDescricao.style.borderColor = "darkgray";
            InputDescricao.placeholder = ""
        }, 5000);
        return;
    };

    if (InputDescricao.value === "") {
        InputDescricao.style.borderColor = "red";
        InputDescricao.placeholder = "Campo Obrigatório"
        setTimeout(function () {
            InputDescricao.style.borderColor = "darkgray";
            InputDescricao.placeholder = ""
        }, 5000);
        return;
    };

    function getFormattedDate() {
        const today = new Date();
        const day = String(today.getDate()).padStart(2, '0');
        const month = String(today.getMonth() + 1).padStart(2, '0'); // Os meses em JavaScript começam do 0, então é preciso adicionar 1
        const year = today.getFullYear();

        return `${day}/${month}/${year}`;
    }
    const nomeUsuario1 = localStorage.getItem('nomeUsuario');


    let ParametrosPlano = {
        "codigo": InputCodigo.value,
        "descricao": InputDescricao.value,
        "inicioVenda": formatDateParametro(InputDataInicialVendas.value),
        "finalVenda": formatDateParametro(InputDataFinallVendas.value),
        "inicioFat": formatDateParametro(InputDataInicialFat.value),
        "finalFat": formatDateParametro(InputDataFinallFat.value),
        "usuario": nomeUsuario1,
        "dataGeracao": getFormattedDate()
    }

    fetch("http://192.168.0.183:8000/pcp/api/Plano", {
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a44pcp22'
        },
        body: JSON.stringify(ParametrosPlano),
    })
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error('Erro ao atribuir pedidos');
            }
        })
        .then(data => {
            console.log(data);
            AbrirColecoes.style.display = "flex";
            ButtonEditarAbc.style.display = "flex";
            ButtonEditarAbc.style.marginLeft = "10px";
            ButtonCurvaVenda.style.display = "flex";
            ButtonCurvaVenda.style.marginLeft = "10px";
            ButtonCurvaVenda.style.width = "90px";
            ButtonCurvaVenda.style.marginLeft = "10px";
            ButtonCurvaVenda.style.width = "70px";
            ButtonEditar.style.display = "flex";
            ButtonCriar.style.display = "none"
            ConsultaColecoesVinculadas();
            ConsultaTiposDeNotasVinculadas();
            ConsultaLotesVinculados();

        })
        .catch(error => {
            console.error(error);
        });
});

//------------------------------------------------------EDITANDO PLANO----------------------------------------------------------------//

const BotaoEditarPlano = document.getElementById("ButtonEditarPlano")

BotaoEditarPlano.addEventListener('click', function () {

    let ParametrosEditarPlano = {
        "descricao": InputDescricao.value,
        "inicioVenda": formatDateParametro(InputDataInicialVendas.value),
        "finalVenda": formatDateParametro(InputDataFinallVendas.value),
        "inicioFaturamento": formatDateParametro(InputDataInicialFat.value),
        "finalFaturamento": formatDateParametro(InputDataFinallFat.value),
    }


    fetch(`http://192.168.0.183:8000/pcp/api/Plano/${InputCodigo.value}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a44pcp22'
        },
        body: JSON.stringify(ParametrosEditarPlano),
    })
        .then(response => {
            if (response.ok) {
                return response.json();
            } else {
                throw new Error('Erro ao atribuir pedidos');
            }
        })
        .then(data => {
            console.log(data);
            alert("Plano Editado com Sucesso")

        })
        .catch(error => {
            console.error(error);
        });
});


//------------------------------------------------------------Coleções-----------------------------------------------------------------//

function ConsultaColecoesVinculadas() {
    fetch(`http://192.168.0.183:8000/pcp/api/ColecoesPlano/${InputCodigo.value}`, {
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
            criarTabelaColecoes(data)
            console.log(data);
        })
        .catch(error => {
            console.error('Erro capturado:', error);
        });
}

function criarTabelaColecoes(listaColecoes) {
    const tabelaColecoes = document.getElementById('TabelaColecoes');
    tabelaColecoes.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckboxColecoes = document.createElement('th');
    const ColunaCodigoColecao = document.createElement('th');
    const ColunaNomeColecao = document.createElement('th');

    colunaCheckboxColecoes.textContent = '';
    ColunaCodigoColecao.textContent = 'Código';
    ColunaNomeColecao.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckboxColecoes);
    cabecalhoRow.appendChild(ColunaCodigoColecao);
    cabecalhoRow.appendChild(ColunaNomeColecao);
    cabecalho.appendChild(cabecalhoRow);
    tabelaColecoes.appendChild(cabecalho);

    listaColecoes.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckboxColecoes = document.createElement('td');
        const ColunaCodigoColecao = document.createElement('td');
        const ColunaNomeColecao = document.createElement('td');

        const checkboxColecoes = document.createElement('input');
        checkboxColecoes.type = 'checkbox';
        checkboxColecoes.value = item["02- colecao"];
        checkboxColecoes.name = 'checkBoxColecoes';
        colunaCheckboxColecoes.appendChild(checkboxColecoes);
        ColunaCodigoColecao.textContent = item["02- colecao"];
        ColunaNomeColecao.textContent = item["03- nomecolecao"];

        row.appendChild(colunaCheckboxColecoes);
        row.appendChild(ColunaCodigoColecao);
        row.appendChild(ColunaNomeColecao);

        tabelaColecoes.appendChild(row);
    });
}

function ConsultaColecoesCsw() {
    fetch(`http://192.168.0.183:8000/pcp/api/PesquisaColecoes`, {
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
            const detalhamentoColecoes = data[0]['1- Detalhamento da Consulta:'];
            criarTabelaColecoesCsw(detalhamentoColecoes)
            console.log(data);
        })
        .catch(error => {
            console.error('Erro capturado:', error);
        });
}


function criarTabelaColecoesCsw(listaColecoesCsw) {
    const tabelaColecoesCsw = document.getElementById('TabelaColecoesCsw');
    tabelaColecoesCsw.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckboxColecoesCsw = document.createElement('th');
    const ColunaColecoes = document.createElement('th');
    const ColunaDescricoes = document.createElement('th');

    colunaCheckboxColecoesCsw.textContent = '';
    ColunaColecoes.textContent = 'Código';
    ColunaDescricoes.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckboxColecoesCsw);
    cabecalhoRow.appendChild(ColunaColecoes);
    cabecalhoRow.appendChild(ColunaDescricoes);
    cabecalho.appendChild(cabecalhoRow);
    tabelaColecoesCsw.appendChild(cabecalho);

    listaColecoesCsw.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckboxColecoesCsw = document.createElement('td');
        const ColunaColecoes = document.createElement('td');
        const ColunaDescricoes = document.createElement('td');

        const checkboxColecoesCsw = document.createElement('input');
        checkboxColecoesCsw.type = 'checkbox';
        checkboxColecoesCsw.value = item["codColecao"];
        checkboxColecoesCsw.name = 'checkboxColecoesCsw';
        colunaCheckboxColecoesCsw.appendChild(checkboxColecoesCsw);
        ColunaColecoes.textContent = item["codColecao"];
        ColunaDescricoes.textContent = item["nome"];

        row.appendChild(colunaCheckboxColecoesCsw);
        row.appendChild(ColunaColecoes);
        row.appendChild(ColunaDescricoes);

        tabelaColecoesCsw.appendChild(row);
    });
}


const BotaoAdicionarColecoes = document.getElementById('AdicionarColecao');
const modalColecoes = document.getElementById('modalColecoesCsw');
const fecharModalColecoesCsw = document.getElementById('fecharmodalColecoesCsw');

BotaoAdicionarColecoes.addEventListener('click', function () {
    ConsultaColecoesCsw();
    modalColecoes.style.display = 'block';
});

fecharModalColecoesCsw.addEventListener('click', function () {
    modalColecoes.style.display = 'none';
});


const botaoSelecionarColecao = document.getElementById('botaoSelecionarColecao')

botaoSelecionarColecao.addEventListener('click', function () {
    const LinhasTabelaColecao = document.getElementById('TabelaColecoesCsw').getElementsByTagName('tr');
    const ColecoesSelecionadas = [];
    const NomeColecaoSelecionadas = [];

    for (let i = 1; i < LinhasTabelaColecao.length; i++) {
        const linha = LinhasTabelaColecao[i];
        const checkboxColecoesSelecionadas = linha.querySelector('input[type="checkbox"]');

        if (checkboxColecoesSelecionadas.checked) {
            const colunasColecoesSelecionadas = linha.getElementsByTagName('td');
            const Colecoes = colunasColecoesSelecionadas[1].textContent.trim();
            const NomeColecoes = colunasColecoesSelecionadas[2].textContent.trim();

            ColecoesSelecionadas.push(Colecoes);
            NomeColecaoSelecionadas.push(NomeColecoes);
        }
    }

    if (ColecoesSelecionadas.length === 0) {
        alert('Nenhuma Coleção selecionada');
        modalColecoes.style.display = 'none';
        modalColecoes.style.display = 'block';
    } else {
        let PassandoColecoes = {
            "codcolecao": ColecoesSelecionadas,
            "nomecolecao": NomeColecaoSelecionadas,
        };
        console.log(ColecoesSelecionadas)
        console.log(NomeColecaoSelecionadas)

        fetch(`http://192.168.0.183:8000/pcp/api/ColecaoPlano/${InputCodigo.value}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a44pcp22'
            },
            body: JSON.stringify(PassandoColecoes),
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                ConsultaColecoesVinculadas(2);
                console.log(data);
            })
            .catch(error => {
                console.error(error);
            });
    }

    modalColecoes.style.display = 'none';
});

const InputBuscaColecoes = document.getElementById('PesquisarColecoes');
const TabelaColecoesPesquisa = document.getElementById('TabelaColecoesCsw');

InputBuscaColecoes.addEventListener('keyup', () => {
    const expressaoColecoes = InputBuscaColecoes.value.trim().toLowerCase();
    const linhasTabelaColecoes = TabelaColecoesPesquisa.getElementsByTagName('tr');

    for (let i = 1; i < linhasTabelaColecoes.length; i++) {
        const linhaColecoes = linhasTabelaColecoes[i];
        const colunasColecoes = linhaColecoes.getElementsByTagName('td');
        let encontrouColecoes = false;

        for (let j = 1; j < colunasColecoes.length; j++) {
            const conteudoColunaColecoes = colunasColecoes[j].textContent.trim().toLowerCase();

            if (conteudoColunaColecoes.includes(expressaoColecoes)) {
                encontrouColecoes = true;
                break;
            }
        }

        if (encontrouColecoes) {
            linhaColecoes.style.display = '';
        } else {
            linhaColecoes.style.display = 'none';
        }
    }
});

//----------------------------------------------------Tipos de Notas----------------------------------------------------------------//

function ConsultaTiposDeNotasVinculadas() {
    fetch(`http://192.168.0.183:8000/pcp/api/NotasPlano/${InputCodigo.value}`, {
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
            criarTabelaNotas(data)
            console.log(data);
        })
        .catch(error => {
            console.error('Erro capturado:', error);
        });
}


function criarTabelaNotas(listaNotas) {
    const tabelaTipoNota = document.getElementById('TabelaTipoNota');
    tabelaTipoNota.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckbox = document.createElement('th');
    const ColunaCodigoNota = document.createElement('th');
    const ColunaNomeNota = document.createElement('th');

    colunaCheckbox.textContent = '';
    ColunaCodigoNota.textContent = 'Código Nota';
    ColunaNomeNota.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckbox);
    cabecalhoRow.appendChild(ColunaCodigoNota);
    cabecalhoRow.appendChild(ColunaNomeNota);
    cabecalho.appendChild(cabecalhoRow);
    tabelaTipoNota.appendChild(cabecalho);

    listaNotas.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckbox = document.createElement('td');
        const ColunaCodigoNota = document.createElement('td');
        const ColunaNomeNota = document.createElement('td');

        const checkbox = document.createElement('input');
        checkbox.type = 'checkbox';
        checkbox.value = item["02- tipo nota"];
        checkbox.name = 'checkBoxNotas';
        colunaCheckbox.appendChild(checkbox);
        ColunaCodigoNota.textContent = item["02- tipo nota"];
        ColunaNomeNota.textContent = item["03- nomeTipoNota"];

        row.appendChild(colunaCheckbox);
        row.appendChild(ColunaCodigoNota);
        row.appendChild(ColunaNomeNota);

        tabelaTipoNota.appendChild(row);
    });
}


function ConsultaNotasCsw() {
    fetch(`http://192.168.0.183:8000/pcp/api/PesquisaTipoNotas`, {
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
            const detalhamentoNotas = data[0]['1- Detalhamento da Consulta:'];
            criarTabelaNotasCsw(detalhamentoNotas)
            console.log(data);
        })
        .catch(error => {
            console.error('Erro capturado:', error);
        });
}


function criarTabelaNotasCsw(listaNotasCsw) {
    const tabelaNotasCsw = document.getElementById('TabelaNotasCSW');
    tabelaNotasCsw.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckboxNotasCsw = document.createElement('th');
    const ColunaNotas = document.createElement('th');
    const ColunaDescricoesNotas = document.createElement('th');

    colunaCheckboxNotasCsw.textContent = '';
    ColunaNotas.textContent = 'Código';
    ColunaDescricoesNotas.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckboxNotasCsw);
    cabecalhoRow.appendChild(ColunaNotas);
    cabecalhoRow.appendChild(ColunaDescricoesNotas);
    cabecalho.appendChild(cabecalhoRow);
    tabelaNotasCsw.appendChild(cabecalho);

    listaNotasCsw.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckboxNotasCsw = document.createElement('td');
        const ColunaNotas = document.createElement('td');
        const ColunaDescricoesNotas = document.createElement('td');

        const checkboxNotasCsw = document.createElement('input');
        checkboxNotasCsw.type = 'checkbox';
        checkboxNotasCsw.value = item["codigo"];
        checkboxNotasCsw.name = 'checkboxNotasCsw';
        colunaCheckboxNotasCsw.appendChild(checkboxNotasCsw);
        ColunaNotas.textContent = item["codigo"];
        ColunaDescricoesNotas.textContent = item["descricao"];

        row.appendChild(colunaCheckboxNotasCsw);
        row.appendChild(ColunaNotas);
        row.appendChild(ColunaDescricoesNotas);

        tabelaNotasCsw.appendChild(row);
    });
}


const BotaoAdicionarNotas = document.getElementById('AdicionarNotas');
const modalNotas = document.getElementById('modalNotaCws');
const fecharModalNotas = document.getElementById('fecharmodalNotasCsw');

BotaoAdicionarNotas.addEventListener('click', function () {
    ConsultaNotasCsw();
    modalNotas.style.display = 'block';
});

fecharModalNotas.addEventListener('click', function () {
    document.getElementById("PesquisarNota").value = ""
    modalNotas.style.display = 'none';
});


const botaoSelecionarNotas = document.getElementById('botaoSelecionarNota')

botaoSelecionarNotas.addEventListener('click', function () {
    const LinhasTabelaNotas = document.getElementById('TabelaNotasCSW').getElementsByTagName('tr');
    const NotasSelecionadas = [];
    const DescricaoNotasSelecionadas = [];

    for (let i = 1; i < LinhasTabelaNotas.length; i++) {
        const linha = LinhasTabelaNotas[i];
        const checkboxNotasSelecionadas = linha.querySelector('input[type="checkbox"]');

        if (checkboxNotasSelecionadas.checked) {
            const colunasNotasSelecionadas = linha.getElementsByTagName('td');
            const Notas = colunasNotasSelecionadas[1].textContent.trim();
            const DescricaoNotas = colunasNotasSelecionadas[2].textContent.trim();

            NotasSelecionadas.push(Notas);
            DescricaoNotasSelecionadas.push(DescricaoNotas);
        }
    }

    if (NotasSelecionadas.length === 0) {
        alert('Nenhum Tipo de Nota selecionado');
        modalNotas.style.display = 'none';
        modalNotas.style.display = 'block';
    } else {
        let PassandoNotas = {
            "tipoNota": NotasSelecionadas,
            "nome": DescricaoNotasSelecionadas,
        };
        console.log(NotasSelecionadas)
        console.log(DescricaoNotasSelecionadas)

        fetch(`http://192.168.0.183:8000/pcp/api/TipoNotaPlano/${InputCodigo.value}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a44pcp22'
            },
            body: JSON.stringify(PassandoNotas),
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                ConsultaTiposDeNotasVinculadas();
                console.log(data);
            })
            .catch(error => {
                console.error(error);
            });
    }

    modalNotas.style.display = 'none';
});


const InputBuscaNota = document.getElementById('PesquisarNota');
const TabelaNotasPesquisa = document.getElementById('TabelaNotasCSW');

InputBuscaNota.addEventListener('keyup', () => {
    const expressaoNota = InputBuscaNota.value.trim().toLowerCase();
    const linhasTabelaNota = TabelaNotasPesquisa.getElementsByTagName('tr');

    for (let i = 1; i < linhasTabelaNota.length; i++) {
        const linha1 = linhasTabelaNota[i];
        const colunas1 = linha1.getElementsByTagName('td');
        let encontrou = false;

        for (let j = 1; j < colunas1.length; j++) {
            const conteudoColuna = colunas1[j].textContent.trim().toLowerCase();

            if (conteudoColuna.includes(expressaoNota)) {
                encontrou = true;
                break;
            }
        }

        if (encontrou) {
            linha1.style.display = '';
        } else {
            linha1.style.display = 'none';
        }
    }
});


//-----------------------------------------------------------Lotes Produção----------------------------------------------------------//

function ConsultaLotesVinculados() {
    fetch(`http://192.168.0.183:8000/pcp/api/LotesPlano/${InputCodigo.value}`, {
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
            criartTabelaLotes(data)
            console.log(data);
        })
        .catch(error => {
            console.error('Erro capturado:', error);
        });
}

function criartTabelaLotes(listaLotes) {
    const tabelaLotes = document.getElementById('TabelaLote');
    tabelaLotes.innerHTML = '';

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckboxLotes = document.createElement('th');
    const ColunaCodigoLotes = document.createElement('th');
    const ColunaNomeLotes = document.createElement('th');

    colunaCheckboxLotes.textContent = '';
    ColunaCodigoLotes.textContent = 'Código';
    ColunaNomeLotes.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckboxLotes);
    cabecalhoRow.appendChild(ColunaCodigoLotes);
    cabecalhoRow.appendChild(ColunaNomeLotes);
    cabecalho.appendChild(cabecalhoRow);
    tabelaLotes.appendChild(cabecalho);

    listaLotes.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckboxLotes = document.createElement('td');
        const ColunaCodigoLotes = document.createElement('td');
        const ColunaNomeLotes = document.createElement('td');

        const checkboxLotes = document.createElement('input');
        checkboxLotes.type = 'checkbox';
        checkboxLotes.value = item["02- lote"];
        checkboxLotes.name = 'checkBoxLotes';
        colunaCheckboxLotes.appendChild(checkboxLotes);
        ColunaCodigoLotes.textContent = item["02- lote"];
        ColunaNomeLotes.textContent = item["03- nomelote"];

        row.appendChild(colunaCheckboxLotes);
        row.appendChild(ColunaCodigoLotes);
        row.appendChild(ColunaNomeLotes);

        tabelaLotes.appendChild(row);
    });
}



function ConsultaLotesCsw() {
    fetch(`http://192.168.0.183:8000/pcp/api/PesquisaLotes`, {
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
            const detalhamentoLotes = data[0]['1- Detalhamento da Consulta:'];
            criarTabelaLotesCsw(detalhamentoLotes)
            console.log(detalhamentoLotes);
        })
        .catch(error => {
            console.error('Erro capturado:', error);
        });
}


function criarTabelaLotesCsw(listaLotesCsw) {
    const tabelaLotesCsw = document.getElementById('TabelaLotesCsw');
    tabelaLotesCsw.innerHTML = ''; // Remove o conteúdo atual da tabela

    const cabecalho = document.createElement('thead');
    const cabecalhoRow = document.createElement('tr');
    const colunaCheckboxLotesCsw = document.createElement('th');
    const ColunaLotes = document.createElement('th');
    const ColunaDescricoesLotes = document.createElement('th');

    colunaCheckboxLotesCsw.textContent = '';
    ColunaLotes.textContent = 'Código';
    ColunaDescricoesLotes.textContent = 'Descrição';

    cabecalhoRow.appendChild(colunaCheckboxLotesCsw);
    cabecalhoRow.appendChild(ColunaLotes);
    cabecalhoRow.appendChild(ColunaDescricoesLotes);
    cabecalho.appendChild(cabecalhoRow);
    tabelaLotesCsw.appendChild(cabecalho);

    listaLotesCsw.forEach(item => {
        const row = document.createElement('tr');
        const colunaCheckboxLotesCsw = document.createElement('td');
        const ColunaLotes = document.createElement('td');
        const ColunaDescricoesLotes = document.createElement('td');

        const checkboxLotesCsw = document.createElement('input');
        checkboxLotesCsw.type = 'checkbox';
        checkboxLotesCsw.value = item["codLote"];
        checkboxLotesCsw.name = 'checkboxLotesCsw';
        colunaCheckboxLotesCsw.appendChild(checkboxLotesCsw);
        ColunaLotes.textContent = item["codLote"];
        ColunaDescricoesLotes.textContent = item["descricao"];

        row.appendChild(colunaCheckboxLotesCsw);
        row.appendChild(ColunaLotes);
        row.appendChild(ColunaDescricoesLotes);

        tabelaLotesCsw.appendChild(row);
    });
}

const BotaoAdicionarLotes = document.getElementById('AdicionarLotes');
const modalLotes = document.getElementById('modalLoteProdCws');
const fecharModalLotes = document.getElementById('fecharmodalLoteProdsCsw');

BotaoAdicionarLotes.addEventListener('click', function () {
    ConsultaLotesCsw();
    modalLotes.style.display = 'block';
});

fecharModalLotes.addEventListener('click', function () {
    document.getElementById("PesquisarLote").value = ""
    modalLotes.style.display = 'none';
});


const botaoSelecionarLotes = document.getElementById('botaoSelecionarLote')

botaoSelecionarLotes.addEventListener('click', function () {
    const LinhasTabelaLotes = document.getElementById('TabelaLotesCsw').getElementsByTagName('tr');
    const LotesSelecionados = [];
    const DescricaoLotesSelecionadas = [];

    for (let i = 1; i < LinhasTabelaLotes.length; i++) {
        const linhaLotes = LinhasTabelaLotes[i];
        const checkboxLotesSelecionados = linhaLotes.querySelector('input[type="checkbox"]');

        if (checkboxLotesSelecionados.checked) {
            const colunasLotesSelecionadas = linhaLotes.getElementsByTagName('td');
            const Lotes = colunasLotesSelecionadas[1].textContent.trim();
            const DescricaoLotes = colunasLotesSelecionadas[2].textContent.trim();

            LotesSelecionados.push(Lotes);
            DescricaoLotesSelecionadas.push(DescricaoLotes);
        }
    }

    if (LotesSelecionados.length === 0) {
        alert('Nenhum Tipo de Nota selecionado');
        modalLotes.style.display = 'none';
        modalLotes.style.display = 'block';
    } else {
        let PassandoLotes = {
            "lote": LotesSelecionados,
            "nome": DescricaoLotesSelecionadas,
        };
        console.log(LotesSelecionados)
        console.log(DescricaoLotesSelecionadas)

        fetch(`http://192.168.0.183:8000/pcp/api/LotePlano/${InputCodigo.value}`, {
            method: 'PUT',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a44pcp22'
            },
            body: JSON.stringify(PassandoLotes),
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                ConsultaLotesVinculados();
                console.log(data);
            })
            .catch(error => {
                console.error(error);
            });
    }

    modalLotes.style.display = 'none';
});

const InputBuscaLote = document.getElementById('PesquisarLote');
const PesquisaTabelaLote = document.getElementById("TabelaLotesCsw")

InputBuscaLote.addEventListener('keyup', () => {
    const expressaoLote = InputBuscaLote.value.trim().toLowerCase();
    const linhasTabelaLote = PesquisaTabelaLote.getElementsByTagName('tr');

    for (let i = 1; i < linhasTabelaLote.length; i++) {
        const linha2 = linhasTabelaLote[i];
        const colunas2 = linha2.getElementsByTagName('td');
        let encontrou = false;

        for (let j = 1; j < colunas2.length; j++) {
            const conteudoColuna = colunas2[j].textContent.trim().toLowerCase();

            if (conteudoColuna.includes(expressaoLote)) {
                encontrou = true;
                break;
            }
        }

        if (encontrou) {
            linha2.style.display = '';
        } else {
            linha2.style.display = 'none';
        }
    }
});




//---------------------------------------------------EXCLUSÃO LOTES------------------------------------------------------------------//
const BotaoExcluirLotes = document.getElementById('LixeiraLotes')

BotaoExcluirLotes.addEventListener('click', function () {
    const LinhasTabelaLotes1 = document.getElementById('TabelaLote').getElementsByTagName('tr');
    const LotesSelecionados1 = [];
    const DescricaoLotesSelecionadas1 = [];

    for (let i = 1; i < LinhasTabelaLotes1.length; i++) {
        const linhaLotes1 = LinhasTabelaLotes1[i];
        const checkboxLotesSelecionados1 = linhaLotes1.querySelector('input[type="checkbox"]');

        if (checkboxLotesSelecionados1.checked) {
            const colunasLotesSelecionadas1 = linhaLotes1.getElementsByTagName('td');
            const Lotes1 = colunasLotesSelecionadas1[1].textContent.trim();


            LotesSelecionados1.push(Lotes1);

        }
    }

    if (LotesSelecionados1.length === 0) {
        alert('Nenhum Tipo de Nota selecionado');
    } else {
        let PassandoLotes1 = {
            "lote": LotesSelecionados1,
        };

        fetch(`http://192.168.0.183:8000/pcp/api/LotePlano/${InputCodigo.value}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a44pcp22'
            },
            body: JSON.stringify(PassandoLotes1),
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                ConsultaLotesVinculados();
                console.log(data);
            })
            .catch(error => {
                console.error(error);
            });
    }
});

//---------------------------------------------------EXCLUSÃO COLEÇÕES------------------------------------------------------------------//
const BotaoExcluirColecoes = document.getElementById('LixeiraColecoes')

BotaoExcluirColecoes.addEventListener('click', function () {
    const LinhasTabelaColecoes1 = document.getElementById('TabelaColecoes').getElementsByTagName('tr');
    const ColecoesSelecionados1 = [];
    const DescricaoColecoesSelecionadas1 = [];

    for (let i = 1; i < LinhasTabelaColecoes1.length; i++) {
        const linhaColecoes1 = LinhasTabelaColecoes1[i];
        const checkboxColecoesSelecionados1 = linhaColecoes1.querySelector('input[type="checkbox"]');

        if (checkboxColecoesSelecionados1.checked) {
            const colunasColecoesSelecionadas1 = linhaColecoes1.getElementsByTagName('td');
            const Colecoes1 = colunasColecoesSelecionadas1[1].textContent.trim();


            ColecoesSelecionados1.push(Colecoes1);

        }
    }

    if (ColecoesSelecionados1.length === 0) {
        alert('Nenhum Tipo de Nota selecionado');
    } else {
        let PassandoColecoes1 = {
            "codigocolecao": ColecoesSelecionados1
        };

        fetch(`http://192.168.0.183:8000/pcp/api/ColecaoPlano/${InputCodigo.value}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a44pcp22'
            },
            body: JSON.stringify(PassandoColecoes1),
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                ConsultaColecoesVinculadas();
                console.log(data);
            })
            .catch(error => {
                console.error(error);
            });
    }
});

//-------------------------------------------EXCLUSÃO TIPOS DE NOTAS-------------------------------------------------------//

const BotaoExcluirNotas = document.getElementById('LixeiraNotas')

BotaoExcluirNotas.addEventListener('click', function () {
    const LinhasTabelaNotas1 = document.getElementById('TabelaTipoNota').getElementsByTagName('tr');
    const NotasSelecionados1 = [];

    for (let i = 1; i < LinhasTabelaNotas1.length; i++) {
        const linhaNotas1 = LinhasTabelaNotas1[i];
        const checkboxNotasSelecionados1 = linhaNotas1.querySelector('input[type="checkbox"]');

        if (checkboxNotasSelecionados1.checked) {
            const colunasNotasSelecionadas1 = linhaNotas1.getElementsByTagName('td');
            const Notas1 = colunasNotasSelecionadas1[1].textContent.trim();


            NotasSelecionados1.push(Notas1);

        }
    }

    if (NotasSelecionados1.length === 0) {
        alert('Nenhum Tipo de Nota selecionado');
    } else {
        let PassandoNotas1 = {
            "tipoNota": NotasSelecionados1
        };

        fetch(`http://192.168.0.183:8000/pcp/api/TipoNotaPlano/${InputCodigo.value}`, {
            method: 'DELETE',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': 'a44pcp22'
            },
            body: JSON.stringify(PassandoNotas1),
        })
            .then(response => {
                if (response.ok) {
                    return response.json();
                } else {
                    throw new Error('Erro ao atribuir pedidos');
                }
            })
            .then(data => {
                ConsultaTiposDeNotasVinculadas();
                console.log(data);
            })
            .catch(error => {
                console.error(error);
            });
    }
});

const InputBuscaPlanos12 = document.getElementById('BotaoPesquisarPlano');
const PesquisaTabelaPlanos12 = document.getElementById("TabelaPlanos")

InputBuscaPlanos12.addEventListener('keyup', () => {
    console.log(InputBuscaPlanos12.value)
    const expressao1 = InputBuscaPlanos12.value.trim().toLowerCase();
    const linhasTabela5 = PesquisaTabelaPlanos12.getElementsByTagName('tr');

    for (let i = 1; i < linhasTabela5.length; i++) {
        const linha5 = linhasTabela5[i];
        const colunas5 = linha5.getElementsByTagName('td');
        let encontrou1 = false;

        for (let j = 1; j < colunas5.length; j++) {
            const conteudoColuna = colunas5[j].textContent.trim().toLowerCase();

            if (conteudoColuna.includes(expressao1)) {
                encontrou1 = true;
                break;
            }
        }

        if (encontrou1) {
            linha5.style.display = '';
        } else {
            linha5.style.display = 'none';
        }
    }
});


const SelecionarPlano = document.getElementById("botaoSelecionarPlano")
  
SelecionarPlano.addEventListener('click', function() {
      const LinhasTabelaPlano = document.getElementById('TabelaPlanos').getElementsByTagName('tr');

  
      for (let i = 1; i < LinhasTabelaPlano.length; i++) {
          const linha2 = LinhasTabelaPlano[i];
          const checkboxPlanoSelecionado = linha2.querySelector('input[type="checkbox"]');
  
          if (checkboxPlanoSelecionado.checked) {
              const colunasPlanoSelecionado = linha2.getElementsByTagName('td');
              const Plano = colunasPlanoSelecionado[1].textContent.trim();
  
              PlanoSelecionado = Plano
          }
      }
  
      if (PlanoSelecionado.length === 0) {
          alert('Nenhum Plano selecionada');
          modalPlano.style.display = 'none';
          modalPlano.style.display = 'block';
      } else {
          document.getElementById("InputPlano").value = PlanoSelecionado;
          modalPlano.style.display = "none"
          carregarDadosFromCodigo();
  
          
      }
  
      modalPlanos.style.display = 'none';
  });


  //------------------------------------------------ FUNÇÃO CURVA ---------------------------------------------------------------
  const modalABC = document.getElementById('modalCurva');
  const fecharModalABC = document.getElementById('fecharmodalCurva');


  
  ButtonEditarAbc.addEventListener('click', function () {
    modalABC.style.display = 'block';
    ObterCurva();
});

fecharModalABC.addEventListener('click', function () {
    modalABC.style.display = 'none';
});

function ObterCurva() {
    const InputA = document.getElementById("InputCurvaA");
    const InputB = document.getElementById("InputCurvaB");
    const InputC = document.getElementById("InputCurvaC");
    const InputC1 = document.getElementById("InputCurvaC1");
    const InputC2 = document.getElementById("InputCurvaC2");
    const InputC3 = document.getElementById("InputCurvaC3");
    const CodPlano = document.getElementById("InputPlano");

    fetch(`http://192.168.0.183:8000/pcp/api/ABCPlano/${CodPlano.value}`, {
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
                throw new Error('Erro ao obter a lista de usuários');
            }
        })
        .then(data => {
            InputA.value = data[0]["a"] * 100;
            InputB.value = data[0]["b"] * 100;
            InputC.value = data[0]["c"] * 100;
            InputC1.value = data[0]["%C1"] * 100;
            InputC2.value = data[0]["%C2"] * 100;
            InputC3.value = data[0]["%C3"] * 100;
            console.log(data);
        })
        .catch(error => {
            console.error(error);
        });
}

const SalvarPercentuais = document.getElementById("SalvarPercentuais")
SalvarPercentuais.addEventListener('click', function () {
    SalvarNovosPercentuais();
});

function SalvarNovosPercentuais() {
    const InputA = document.getElementById("InputCurvaA");
    const InputB = document.getElementById("InputCurvaB");
    const InputC = document.getElementById("InputCurvaC");
    const InputC1 = document.getElementById("InputCurvaC1");
    const InputC2 = document.getElementById("InputCurvaC2");
    const InputC3 = document.getElementById("InputCurvaC3");
    const CodPlano = document.getElementById("InputPlano");

    const valorA = parseFloat(InputA.value);
    const valorB = parseFloat(InputB.value);
    const valorC = parseFloat(InputC.value);
    const valorC1 = parseFloat(InputC1.value);
    const valorC2 = parseFloat(InputC2.value);
    const valorC3 = parseFloat(InputC3.value);


    if (valorA + valorB + valorC > 100) {
        alert("A soma dos Percentuais de A, B e C não podem ultrapassar 100%");
        return;
    }

    if (valorA + valorB + valorC < 100) {
        alert("A soma dos Percentuais de A, B e C não podem ser menor que 100%");
        return;
    }

    if (valorC1 + valorC2 + valorC3 > 100) {
        alert("A soma dos Percentuais de C1, C2 e C3 não podem ultrapassar 100%");
        return;
    }

    if (valorC1 + valorC2 + valorC3 < 100) {
        alert("A soma dos Percentuais de C1, C2 e C3  não podem ser menor que 100%");
        return;
    }

    const dadosPercentuais = {
        "plano": CodPlano.value,
        "a": valorA / 100,
        "b": valorB / 100,
        "c": valorC / 100,
        "c1": valorC1 / 100,
        "c2": valorC2 / 100,
        "c3": valorC3 / 100,



    }

    fetch("http://192.168.0.183:8000/pcp/api/EditarABCPlano", {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'Authorization': 'a44pcp22'
        },
        body: JSON.stringify(dadosPercentuais),
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
        })
        .catch(error => {
            console.error(error);
        });
};


// Pega todos os campos de entrada que você deseja controlar
const inputFields = [
    document.getElementById("InputCurvaA"),
    document.getElementById("InputCurvaB"),
    document.getElementById("InputCurvaC"),
    document.getElementById("InputCurvaC1"),
    document.getElementById("InputCurvaC2"),
    document.getElementById("InputCurvaC3")
];

// Adiciona os event listeners para cada campo de entrada
inputFields.forEach((input, index) => {
    input.addEventListener("keydown", event => {
        const key = event.key;

        if (key === "ArrowUp") {
            event.preventDefault(); // Impede a ação padrão do ArrowUp
            const previousIndex = index > 0 ? index - 1 : inputFields.length - 1;
            inputFields[previousIndex].focus();
        } else if (key === "ArrowDown" || key === "Enter") {
            event.preventDefault(); // Impede a ação padrão do ArrowDown e Enter
            const nextIndex = index < inputFields.length - 1 ? index + 1 : 0;
            inputFields[nextIndex].focus();
        }
    });
});





