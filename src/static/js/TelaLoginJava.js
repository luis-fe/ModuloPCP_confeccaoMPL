function Usuarios() {
  const username = document.getElementById('usuario').value;
  const password = document.getElementById('password').value;
  const apiUrl = `http:192.168.0.183:8000/api/UsuarioSenha?codigo=${username}&senha=${password}`;

  fetch(apiUrl, {
    method: 'GET',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': 'a40016aabcx9'
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
      // Prossiga com o código
      window.location.href = "/TelaPrincipal";
    } else {
      // Bloqueia o código
      console.log('Usuário inválido. Acesso negado.');
    }
  })
  .catch(function(error) {
    // Lidar com erros
    console.error(error);
  });
}


function checkEnter(event, nextInputId) {
  const VerificaUser = document.getElementById('usuario').value;
  if (event.key === "Enter") {
    if (VerificaUser === '') {
      alert('Informe o login');
    } else {
      event.preventDefault();
      document.getElementById(nextInputId).focus();
    }
  }
}


const BotaoCadUsuario = document.getElementById('BotaoCadUsuario');
const modal = document.getElementById('modal');
const fecharModal = document.getElementById('fecharModal');

BotaoCadUsuario.addEventListener('click', function() {
  modal.style.display = 'block';
});

fecharModal.addEventListener('click', function() {
  modal.style.display = 'none';
  document.getElementById('InputUsuario').value = '';
  document.getElementById('InputLogin').value = '';
  document.getElementById('InputSenha').value = '';
});


function checkEnterModal(event, nextInputId, Message) {
  if (event.key === "Enter") {
    const inputValue = event.target.value;
    if (inputValue === '') {
      alert(Message);
    } else {
      event.preventDefault();
      document.getElementById(nextInputId).focus();
    }
  }
}

function salvarUsuario() {
  const inputUsuario = document.getElementById('InputUsuario');
  const inputLogin = document.getElementById('InputLogin');
  const inputSenha = document.getElementById('InputSenha');

  if (inputUsuario.value === '' || inputLogin.value === '' || inputSenha.value === '') {
    if (inputUsuario.value === '') {
      inputUsuario.classList.add('error');
    } else {
      inputUsuario.classList.remove('error');
    }

    if (inputLogin.value === '') {
      inputLogin.classList.add('error');
    } else {
      inputLogin.classList.remove('error');
    }

    if (inputSenha.value === '') {
      inputSenha.classList.add('error');
    } else {
      inputSenha.classList.remove('error');
    }

    setTimeout(function() {
      inputUsuario.classList.remove('error');
      inputLogin.classList.remove('error');
      inputSenha.classList.remove('error');
    }, 5000);
  } else {
    // Código para salvar o usuário
  }
}