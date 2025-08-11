fetch(`http://192.168.0.183:8000/pcp/api/StatusPlano/1`, {
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
            console.log(data)
        })
        .catch(error => {
            console.error(error);
        });
 