function Usuarios() {
    const username = document.getElementById('InputLogin').value;
    const password = document.getElementById('InputSenha').value;
    const apiUrl = `http://192.168.0.183:8000/pcp/api/UsuarioSenha?codigo=${username}&senha=${password}`;
    const UsuarioInvalido = document.getElementById('UsuarioInválido')
  
    fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'a44pcp22'
      }
    })
    .then(function(response) {
      if (response.ok) {
        return response.json();
      } else {
        return response.json();
      }
    })
    .then(function(data) {
      // Verifica o valor do campo "status" na resposta da API
      if (data.status === true) {
        const nomeUsuario = data.Usuario;
        localStorage.setItem('nomeUsuario', nomeUsuario);
        window.location.href = "/TelaPrincipal";
      } else {
        UsuarioInvalido.style.display = "block"
      }
    })
    .catch(function(error) {
      // Lidar com erros
      console.error(error);
    });
    
  }

  

 //------------------------------------------------------------------------------Criação Tela Inicial-----------------------------------------------------------------------//
const apiUrl = 'http://192.168.0.183:8000/pcp/api/Usuarios';
const Token = "a44pcp22"
let indiceExibicao = 0;
let totalUsuarios = 0;
let totalPaginas = 0;
let paginaAtual = 1;
const ItensPag = 20;




function criarTabelaUsuarios(listaUsuarios) {
  const tabela = document.getElementById('TabelaUsuarios');
  tabela.innerHTML = ''; 

  const cabecalho = document.createElement('thead');
  const cabecalhoRow = document.createElement('tr');
  const colunaCheckbox = document.createElement('th');
  const colunaCodigo = document.createElement('th');
  const colunaNome = document.createElement('th');

  colunaCheckbox.textContent = '';
  colunaCodigo.textContent = 'Login';
  colunaNome.textContent = 'Nome Usuário';

  colunaCheckbox.style.width = '30px';
  colunaCodigo.style.width = '100px';
  colunaNome.style.width = '250px';

  cabecalhoRow.appendChild(colunaCheckbox);
  cabecalhoRow.appendChild(colunaCodigo);
  cabecalhoRow.appendChild(colunaNome);
  cabecalho.appendChild(cabecalhoRow);
  tabela.appendChild(cabecalho);

  // Preenche a tabela com os dados dos usuários
  const usuariosExibicao = listaUsuarios.slice(indiceExibicao, indiceExibicao + ItensPag);

  usuariosExibicao.forEach(usuario => {
    const row = document.createElement('tr');
    const colunaCheckbox = document.createElement('td');
    const colunaCodigo = document.createElement('td');
    const colunaNome = document.createElement('td');
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.value = usuario.codigo;
    checkbox.name = 'checkboxUsuario';
    checkbox.addEventListener('change', function(event) {
      const checkboxes = document.getElementsByName('checkboxUsuario');
      checkboxes.forEach(box => {
        if (box !== event.target) {
          box.checked = false;
        }
      });
    });
    colunaCheckbox.appendChild(checkbox);
    colunaCodigo.textContent = usuario.codigo;
    colunaNome.textContent = usuario.nome;
    row.appendChild(colunaCheckbox);
    row.appendChild(colunaCodigo);
    row.appendChild(colunaNome);
    tabela.appendChild(row);
  });

}

async function USUARIOS(metodo, dados = null, parametro ) {
  const LabelPaginas = document.getElementById("LabelContagemUsuarios");

  const fetchOptions = {
    method: metodo,
    headers: {
      'Content-Type': 'application/json',
      'Authorization': Token
    }
  };

  if (metodo !== 'GET' || metodo !== 'DELETE') {
    fetchOptions.body = JSON.stringify(dados);
  }

  if (metodo === 'GET') {
    fetchOptions.body = null;
  }

  try {
    const response = await fetch(`${apiUrl}${parametro}`, fetchOptions);
    
    if (response.ok) {
      const data = await response.json();
      if (metodo === 'GET') {
        totalUsuarios = data.length;
        totalPaginas = Math.ceil(totalUsuarios / ItensPag);
        LabelPaginas.textContent = `Página ${paginaAtual} de ${totalPaginas}`;
        criarTabelaUsuarios(data);
      }
    } else {
      const errorData = await response.json();
      console.error(errorData);
    }
  } catch (error) {
    console.error(error);
  }
}

//---------------------------------------------------------------------------Botões de Paginamento------------------------------------------------------------------------------------------------------//

const BotaoProximaPaginaUsuarios = document.getElementById("ButtonProximaPaginaUsuarios");

BotaoProximaPaginaUsuarios.addEventListener("click", function (){
  if(paginaAtual >= totalPaginas){
    alert("Última Página!")
  } else{
    indiceExibicao = indiceExibicao + ItensPag;
    paginaAtual = paginaAtual + 1;
    USUARIOS("GET", dados = null, "");

  }
});

const BotaoUltimaPagina = document.getElementById("ButtonUltimaPaginaUsuarios");

BotaoUltimaPagina.addEventListener("click", function (){
  if(paginaAtual >= totalPaginas){
    alert("Última Página!")
  } else{
    indiceExibicao = indiceExibicao + ((parseInt(totalPaginas) - 1) * ItensPag);
    paginaAtual = totalPaginas;
    USUARIOS("GET", dados = null, "");

  }
});

const BotaoPrimeiraPagina = document.getElementById("ButtonPrimeiraPaginaUsuarios");

BotaoPrimeiraPagina.addEventListener("click", function (){
  if(paginaAtual === 1){
    alert("Primeira Página!")
  } else{
    indiceExibicao = 0;
    paginaAtual = 1;
    USUARIOS("GET", dados = null, "");

  }
});

const BotaoPaginaAnterior = document.getElementById("ButtonPaginaAnteriorUsuarios");

BotaoPaginaAnterior.addEventListener("click", function (){
  if(paginaAtual === 1){
    alert("Primeira Página!")
  } else{
    indiceExibicao = indiceExibicao - ItensPag;
    paginaAtual = paginaAtual - 1;
    USUARIOS("GET", dados = null, "");

  }
});

//-------------------------------------------------------------------------------------Funcao Novo Usuário-----------------------------------------------------------------------------------------------------//

document.getElementById("BotaoCadUsuario").addEventListener("click",  function() {
  AbrirModal("modalNovo")
});

function aplicarBordaVermelha(element) {
  element.style.border = '1px solid red';
}

function removerBordaVermelha(element) {
  element.style.border = '';
}

document.getElementById("salvarNovoUsuário").addEventListener("click", async function(){
  const modalNovoContent = document.querySelector('.modalNovo-content');
  const inputs = modalNovoContent.querySelectorAll('input[type="text"], input[type="password"]');
  let FaltaInformacaoNovo = false;
  const dadosNovos = {
    "codigo": document.getElementById("InputLogin").value,
    "nome": document.getElementById("InputUsuario").value,
    "senha": document.getElementById("InputSenha").value
  }

  inputs.forEach(input => {
    if (input.value === '') {
      FaltaInformacaoNovo = true;
      input.style.borderColor = 'red'; // Aplica a borda vermelha diretamente
      setTimeout(() => {
        input.style.borderColor = ''; // Remove a borda vermelha após 10 segundos
      }, 10000);
      console.log(`Input ${input.id} não está preenchida`);
    }
  });

  console.log(FaltaInformacaoNovo)

  if (!FaltaInformacaoNovo) {
    await USUARIOS("PUT", dadosNovos, "");
    await USUARIOS("GET", dados = null, "");
    FecharModalNovo();
  }
});




//--------------------------------------------------------------------------Função Editar Usuário--------------------------------------------------------------------------------------------------------//

function CapturarLoginSelecionado() {
  const linhasTabela = document.getElementById('TabelaUsuarios').getElementsByTagName('tr');

  for (let i = 1; i < linhasTabela.length; i++) {
    const linha = linhasTabela[i];
    const checkbox = linha.querySelector('input[type="checkbox"]');
      
    if (checkbox.checked) {
      const colunas = linha.getElementsByTagName('td');
      const nome = colunas[2].textContent.trim();
      const usuario = colunas[1].textContent.trim();
          
      return { nome, usuario }; // Retorna um objeto com as propriedades nome e usuario
    }
  }

  return null;
}


document.getElementById("ButtonEditar").addEventListener("click", function(){
  const Resultado = CapturarLoginSelecionado();
  

  if (Resultado === null) {
    alert("Nenhum Usuário selecionado!");
  } else {
    const nomeUsuario = (Resultado.nome);
     const Usuario = (Resultado.usuario);
    console.log(Usuario)
    fetch(`http://192.168.0.183:8000/pcp/api/Usuarios/${Usuario}`, {
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
          console.log(data);
          const Senha = data["2-Senha"];
          console.log(Senha);
          document.getElementById("InputSenhaEditar").value = Senha
          document.getElementById("InputConfirmarSenha").value = Senha
          
      })
      .catch(error => {
          console.error(error);
          FecharLoadingCurva(); // Remova esta linha do bloco catch
      });

    AbrirModal("ModalEditar");
    document.getElementById("InputUsuarioEditar").value = nomeUsuario
  }
});

document.getElementById("salvarEdicao").addEventListener("click", async function ()  {
  const inputs = document.querySelector('.ModalEditar-content').querySelectorAll('input[type="text"], input[type="password"]');
  let FaltaInformacaoEditar = false;
  let SenhasIguais = false
  const Resultado = CapturarLoginSelecionado();
  const Usuario = (Resultado.usuario);


  dadosEdicao = {
    "nome": document.getElementById("InputUsuarioEditar").value,
    "senha": document.getElementById("InputConfirmarSenha").value
  };

  inputs.forEach(input => {
    if (input.value === '') {
      FaltaInformacaoEditar = true;
      input.style.borderColor = 'red'; // Aplica a borda vermelha diretamente
      setTimeout(() => {
        input.style.borderColor = ''; // Remove a borda vermelha após 10 segundos
      }, 10000);
      console.log(`Input ${input.id} não está preenchida`);
    }
  });

  if(document.getElementById("InputSenhaEditar").value !== document.getElementById("InputConfirmarSenha").value){
    SenhasIguais = true;
    document.getElementById("LabelSenhasConferidas").style.display = "block"
    console.log(SenhasIguais)
  } else {
    SenhasIguais = false
  }


  if (!FaltaInformacaoEditar && !SenhasIguais) {
    await USUARIOS("POST", dadosEdicao, `/${Usuario}`);
    FecharModalEditar();
    await USUARIOS("GET", dados = null, "");
  }

  
});

//------------------------------------------------------------------Excluir Usuários----------------------------------------------------------------------------------//

document.getElementById("ButtonRemover").addEventListener("click", function (){
  const Resultado = CapturarLoginSelecionado();
  if (Resultado === null) {
    alert("Nenhum Usuário selecionado!");
  } else {
    const UsuarioSelecionado = (Resultado.usuario);
    console.log(UsuarioSelecionado)
    AbrirModal("ModalExcluir")}
});

document.getElementById("UsuarioSim").addEventListener("click", async function (){
  const Resultado = CapturarLoginSelecionado();
  const UsuarioSelecionado = (Resultado.usuario);
  console.log(typeof(UsuarioSelecionado))
  
  await USUARIOS("DELETE", dados = null, `/${UsuarioSelecionado}`),
  FecharModalExcluir();
  await USUARIOS("GET", dados = null, "");
});

document.getElementById("UsuarioNão").addEventListener("click", function (){
  FecharModalExcluir();
});





window.addEventListener('load', function(){
  paginaAtual = 1;
  USUARIOS("GET", dados = null, "");
  const PaginamentoUsuarios = document.querySelector(".PaginamentoUsuarios");
  PaginamentoUsuarios.style.display = "flex";
  
  }
  );


  function AbrirModal(Modal){;
    document.getElementById(Modal).style.display = "flex"
  };

  document.getElementById("fecharModal").addEventListener("click", function() {
    FecharModalNovo();
  });

  document.getElementById("FecharModalEditar").addEventListener("click", function() {
    FecharModalEditar();
  });

  document.getElementById("FecharModalExcluir").addEventListener("click", function() {
    FecharModalExcluir();
  });

  function FecharModalNovo(){
    document.getElementById("modalNovo").style.display = "none";
    document.getElementById("InputUsuario").value = "";
    document.getElementById("InputLogin").value = "";
    document.getElementById("InputSenha").value = "";
  };

  function FecharModalEditar(){
    document.getElementById("ModalEditar").style.display = "none";
    document.getElementById("InputUsuarioEditar").value = "";
    document.getElementById("InputSenhaEditar").value = "";
    document.getElementById("InputConfirmarSenha").value = "";
    
  };

  function FecharModalExcluir(){
    document.getElementById("ModalExcluir").style.display = "none";
  };
  













