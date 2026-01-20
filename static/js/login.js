const API_BASE_URL = "http://10.162.0.53:9000/pcp/api";
const AUTH_HEADER = "a44pcp22";

document.addEventListener("DOMContentLoaded", () => {

    const matriculaInput = document.getElementById("matricula");
    const nomeInput = document.getElementById("nome");
    const senhaInput = document.getElementById("senha");
    const toggleSenha = document.getElementById("toggleSenha");
    const btnEntrar = document.getElementById("btnEntrar");

    senhaInput.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
        event.preventDefault();
        btnEntrar.click();
    }
});

    const novaSenhaContainer = document.getElementById("novaSenhaContainer");
    const novaSenhaInput = document.getElementById("novaSenha");

    /* =====================================================
       BUSCAR NOME DO USUÁRIO (ENTER OU TAB)
    ===================================================== */
    matriculaInput.addEventListener("keydown", async (event) => {
        if (event.key === "Enter" || event.key === "Tab") {
            event.preventDefault();

            const matricula = matriculaInput.value.trim();
            if (!matricula) return;

            const nome = await buscarNomeUsuarioAPI(matricula);

            if (nome) {
                nomeInput.value = nome;
                senhaInput.focus();
            } else {
                nomeInput.value = "";
                alert("Usuário não encontrado");
            }
        }
    });

    /* =====================================================
       MOSTRAR / OCULTAR SENHA
    ===================================================== */
    toggleSenha.addEventListener("click", () => {
        const icone = toggleSenha.querySelector("i");

        if (senhaInput.type === "password") {
            senhaInput.type = "text";
            icone.classList.replace("bi-eye-slash", "bi-eye");
        } else {
            senhaInput.type = "password";
            icone.classList.replace("bi-eye", "bi-eye-slash");
        }
    });

    /* =====================================================
       AUTENTICAÇÃO
    ===================================================== */
    btnEntrar.addEventListener("click", autenticarUsuario);

    async function autenticarUsuario() {
        const matricula = matriculaInput.value.trim();
        const senha = senhaInput.value.trim();

        if (!matricula || !senha) {
            alert("Informe matrícula e senha");
            return;
        }

        try {
            const response = await fetch(
                `${API_BASE_URL}/autentificarUsuarioSenhaMetas?matricula=${matricula}&senha=${senha}`,
                {
                    method: "GET",
                    headers: {
                        "Authorization": AUTH_HEADER
                    }
                }
            );

            if (!response.ok) throw new Error();

            const data = await response.json();
            const resultado = data[0];

            /* ===== CASO 1 — CADASTRAR NOVA SENHA ===== */
            if (resultado.Mensagem === "Cadastar Nova Senha") {
                senhaInput.closest(".input-group").classList.add("d-none");
                novaSenhaContainer.classList.remove("d-none");

                btnEntrar.textContent = "Cadastrar Senha";
                btnEntrar.removeEventListener("click", autenticarUsuario);
                btnEntrar.addEventListener("click", cadastrarNovaSenha);
                return;
            }

            /* ===== CASO 2 — SENHA INCORRETA ===== */
            if (resultado.Mensagem === "Senhas nao Confere") {
                alert("Senha incorreta");
                return;
            }

            /* ===== CASO 3 — AUTENTICADO ===== */
            if (resultado.status === true) {
    // Salva a matrícula do usuário logado

if (resultado.status === true) {
    localStorage.setItem("matriculaUsuario", matricula);

    const destino = localStorage.getItem("destinoPosLogin");

    if (destino) {
        localStorage.removeItem("destinoPosLogin");
        window.location.href = destino;
    } else {
        window.location.href = "../TiposdeNotas.html";
    }
}


}
        } catch (error) {
            console.error("Erro na autenticação", error);
            alert("Erro ao autenticar usuário");
        }
    }

    /* =====================================================
       CADASTRAR NOVA SENHA (POST)
    ===================================================== */
    async function cadastrarNovaSenha() {
        const matricula = matriculaInput.value.trim();
        const nome = nomeInput.value.trim();
        const novaSenha = novaSenhaInput.value.trim();

        if (!novaSenha) {
            alert("Digite a nova senha");
            return;
        }

        try {
            const response = await fetch(
                `${API_BASE_URL}/post_salvarUsuarioSenha`,
                {
                    method: "POST",
                    headers: {
                        "Authorization": AUTH_HEADER,
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        matricula: matricula,
                        nome: nome,
                        senha: novaSenha
                    })
                }
            );

            if (!response.ok) throw new Error();

            alert("Senha cadastrada com sucesso!");
            window.location.reload();

        } catch (error) {
            console.error("Erro ao cadastrar nova senha", error);
            alert("Erro ao cadastrar nova senha");
        }
    }
});

/* =====================================================
   BUSCAR NOME DO USUÁRIO
===================================================== */
async function buscarNomeUsuarioAPI(matricula) {
    try {
        const response = await fetch(
            `${API_BASE_URL}/devolver_nome_usuario?matricula=${matricula}`,
            {
                method: "GET",
                headers: {
                    "Authorization": AUTH_HEADER
                }
            }
        );

        if (!response.ok) throw new Error();

        const data = await response.json();
        return data[0]?.nome || null;

    } catch (error) {
        console.error("Erro ao buscar usuário", error);
        return null;
    }
}
