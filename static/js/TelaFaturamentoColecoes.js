

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
