const ApiConsulta = 'http://192.168.0.183:8000/pcp/api/CargaOPs';
        const ApiJustificativa = 'http://192.168.0.183:8000/pcp/api/CadastrarJustificativa';
        const ModalPendencia = document.getElementById("ModalPendencia");
        const DivPendencias = document.getElementById("DivPendencias");
        const ApiColecoes = 'http://192.168.0.183:8000/pcp/api/DistinctColecao';
        const Token = 'a44pcp22';
        let VarOp = '';
        let VarFase = '';
        let VarObs = '';
        let contem = '';
        let ordenacao = '';
        let colecoesSelecionadas = [];

        const LabelOp = document.getElementById('NumeroOP');
        const InputObs = document.getElementById('InputJustificativa');
        const SalvarObs = document.getElementById('SalvarObs');
        var divCorpo = document.getElementById('Corpo');
        const ModalJustificativa = document.getElementById('ModalJustificativa')


        $(document).ready(async () => {
            await $('#modalLoading').modal('show');
            ConsultarOps(ApiConsulta, '1', `${contem}`, ordenacao, colecoesSelecionadas);
            ConsultaColecoes();
        });

        $('#cardInfos5').click(async function() {
            await $('#modalLoading').modal('show');
            ConsultarOps(ApiConsulta, '1', `${contem}/atrasad`, ordenacao, colecoesSelecionadas);
        });

        $('#BotaoFiltros').click(function() {
            $('#ModalFiltros').modal('show');
        });

        document.getElementById('FecharJustificativa').addEventListener('click', function() {
            ModalJustificativa.style.display = 'none'
        })

        document.getElementById('SalvarObs').addEventListener('click', function() {

            salvarJustificativa()

        })

        function ConsultarOps(Api, Empresa, Filtro, Classificacao, Colecao) {

            const Dados = {
                "empresa": Empresa,
                "filtro": Filtro,
                "classificar": Classificacao,
                "colecao": Colecao
            };

            console.log(Dados)
            $.ajax({
                url: Api,
                method: 'POST',
                dataType: 'json',
                headers: {
                    'Authorization': Token
                },
                data: JSON.stringify(Dados),
                contentType: 'application/json',
                success: function(response) {
                    $('#ModalFiltros').modal('hide');
                    $('#modalLoading').modal('hide');
                    console.log('Resposta da API:', response);
                    $('#Corpo').empty();

                    $('#text1').text(parseFloat(response[0]['1-Total OP']).toLocaleString('pt-BR'));
                    $('#text2').text(parseFloat(response[0]['0-Total DE pçs']).toLocaleString('pt-BR'));
                    $('#text3').text(parseInt(response[0]['1-Total OP']) - parseInt(response[0]['2- OPs Atrasadas']) - parseInt(response[0]['2.1- OPs Atencao']).toLocaleString('pt-BR'));
                    $('#text4').text(parseFloat(response[0]['2.1- OPs Atencao']).toLocaleString('pt-BR'));
                    $('#text5').text(parseFloat(response[0]['2- OPs Atrasadas']).toLocaleString('pt-BR'));
                    response[0]['3 -Detalhamento'].forEach(function(item) {
                        function getColorClass(status) {
                            switch (status) {
                                case '0-Normal':
                                    return 'bg-success';
                                case '2-Atrasado':
                                    return 'bg-danger';
                                case '1-Atencao':
                                    return 'bg-warning';
                                default:
                                    return '';
                            }
                        }

                        var colorClass = getColorClass(item.status);
                        var divCard = $('<div class="col-sm-6 col-md-3">');
                        divCard.attr('data-op', item.numeroOP);
                        divCard.attr('data-fase', item.codFase);
                        var card = $('<div class="card" id="CardCorpo">');
                        var cardBody = $('<div class="card-body ' + colorClass + '" id="Teste">');
                        var cardTitle = $('<h5 class="card-title">').text('Fase: ' + item.codFase + ' - ' + item.nomeFase);
                        var cardText = $('<p class="card-text" id="Maior">').html('<strong>Numero Op: ' + item.numeroOP + '/ Qtd: ' + item['Qtd Pcs'] + 'Pçs </strong>');
                        var cardText2 = $('<p class="card-text" id="Maior">').html('<strong>Engenharia: ' + item.codProduto + '</strong>');
                        var cardText3 = $('<p class="card-text" id="Menor">').html('<strong>' + item.descricao + '</strong>');
                        var cardText4 = $('<p class="card-text" id="Maior">').html('<strong>Tipo Op: ' + item.codTipoOP + '</strong>');
                        var cardText5 = $('<p class="card-text" id="Maior">').html('<strong>Responsável: ' + item.responsavel + '</strong>');
                        var cardText6 = $('<p class="card-text" id="Maior">').html('<strong>Meta: ' + item.meta + '</strong>');
                        var cardText7 = $('<p class="card-text" id="Maior">').html('<strong>Dias na Fase: ' + item['dias na Fase'] + '</strong>');
                        var cardText8 = $('<p class="card-text" id="justificativa">').html('<strong>Justificativa: ' + item.justificativa + '</strong>');
                        var spanElement = $('<span>').attr('style', item['Status Aguardando Partes'] === 'PENDENTE' || item['Status Aguardando Partes'] === 'OK' ? 'font-size: 50px; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center;' : '').html(
                            (item['Status Aguardando Partes'] === 'PENDENTE' ? '⚠️' : '') +
                            (item['Status Aguardando Partes'] === 'OK' ? '✅' : '')
                        );

                        if (item.prioridade) {
                            const urgenteSymbol = document.createElement('div');
                            urgenteSymbol.innerHTML = item.prioridade;
                            urgenteSymbol.style.backgroundColor = '#FFFF00';
                            urgenteSymbol.style.color = 'black';
                            urgenteSymbol.style.border = '1px solid black';
                            urgenteSymbol.style.fontSize = '22px';
                            urgenteSymbol.style.marginLeft = '-18px';
                            urgenteSymbol.style.width = '120px';
                            cardBody.append(urgenteSymbol);
                        }

                        cardBody.append(cardTitle, cardText, cardText2, cardText3, cardText4, cardText5, cardText6, cardText7, cardText8, spanElement);
                        card.append(cardBody);
                        divCard.append(card)
                        $('#Corpo').append(divCard);

                        divCard.on('mouseover', () => {
                            if (item['Status Aguardando Partes'] === 'PENDENTE') {
                                mostrarModalPendente(card[0], item);
                            }
                        });

                        divCard.on('mouseout', () => {
                            fecharModalPendente();
                        });

                        divCard.click(async function() {
                            try {
                                await Justificativas(item.numeroOP, item.codFase);

                                LabelOp.innerHTML = `OP: ${item.numeroOP}`;
                                InputObs.value = `${VarObs}`;
                                InputObs.focus();

                                var rect = $(this)[0].getBoundingClientRect();
                                var scrollTop = $('#Corpo').scrollTop();
                                var topOffset = rect.top + scrollTop;

                                $('#ModalJustificativa').css({
                                    top: Math.min(rect.bottom + scrollTop, $('#Corpo').offset().top + $('#Corpo').height() - $('#ModalJustificativa').height()) + 'px',
                                    left: rect.left + 'px',
                                    display: 'block'
                                });

                                VarOp = item.numeroOP;
                                VarFase = item.codFase;
                            } catch (error) {
                                console.error('Erro ao aguardar a justificativa:', error);
                            }
                        });

                    });
                },
                error: function(xhr, status, error) {
                    console.error('Erro ao chamar a API:', status, error);
                    $('#modalLoading').modal('hide');
                }
            });
        }

        function atualizarJustificativaCard(op, fase, justificativa) {
            const card = document.querySelector(`#Corpo div[data-op="${op}"][data-fase="${fase}"]`);
            console.log(card)
            if (card) {
                const justificativaElement = card.querySelector('.card-text#justificativa');
                if (justificativaElement) {
                    justificativaElement.innerHTML = `<strong>Justificativa: </strong>${justificativa}`;
                } else {
                    console.error('Elemento da justificativa não encontrado.');
                }
            } else {
                console.error('Card não encontrado.');
            }
        }

        function ConsultaColecoes() {

            $.ajax({
                url: ApiColecoes,
                method: 'GET',
                dataType: 'json',
                headers: {
                    'Authorization': Token
                },
                contentType: 'application/json',
                success: function(response) {
                    const divOpcoes = $('#divOpcoes');
                    divOpcoes.empty();
                    response.forEach(opcao => {
                        // Crie o elemento do checkbox
                        const checkbox = $('<div class="form-check">')
                            .append(
                                $('<input class="form-check-input" type="checkbox">')
                                .attr('value', opcao.COLECAO)
                                .attr('id', `checkbox${opcao.COLECAO}`)
                            )
                            .append(
                                $('<label class="form-check-label">')
                                .attr('for', `checkbox${opcao.COLECAO}`)
                                .text(opcao.COLECAO) // Corrigido para usar opcao.COLECAO
                            );

                        // Adicione o checkbox à div de opções
                        divOpcoes.append(checkbox);
                    });
                },
                error: function(xhr, status, error) {
                    console.error('Erro ao chamar a API:', status, error);
                    Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                    InputObs.value = ''; // Limpar o conteúdo do InputObs
                    document.getElementById('ModalJustificativa').style.display = "none"
                }
            });
        }

        function salvarJustificativa() {
            const dadoApi = {
                "ordemProd": VarOp,
                "fase": VarFase,
                "justificativa": InputObs.value
            };
            console.log(dadoApi)

            $.ajax({
                url: ApiJustificativa,
                method: 'PUT',
                dataType: 'json',
                headers: {
                    'Authorization': Token
                },
                data: JSON.stringify(dadoApi),
                contentType: 'application/json',
                success: function(response) {
                    Swal.fire({
                        title: "Justificativa Inserida",
                        icon: "success",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                    InputObs.value = ''; // Limpar o conteúdo do InputObs
                    document.getElementById('ModalJustificativa').style.display = "none";
                    atualizarJustificativaCard(VarOp, VarFase, dadoApi.justificativa);
                },
                error: function(xhr, status, error) {
                    console.error('Erro ao chamar a API:', status, error);
                    Swal.fire({
                        title: "Erro",
                        icon: "error",
                        showConfirmButton: false,
                        timer: 3000,
                    });
                    InputObs.value = ''; // Limpar o conteúdo do InputObs
                    document.getElementById('ModalJustificativa').style.display = "none"
                }
            });
        }

        async function Justificativas(Op, Fase) {
            await $.ajax({
                url: `http://192.168.0.183:8000/pcp/api/ConsultarJustificativa?ordemProd=${Op}&fase=${Fase}`,
                method: 'GET',
                dataType: 'json',
                headers: {
                    'Authorization': Token
                },
                contentType: 'application/json',
                success: function(response) {
                    VarObs = response[0]['justificativa'];
                },
                error: function(xhr, status, error) {
                    console.error('Erro ao chamar a API:', status, error);
                },
                complete: function() {
                    $('#modalLoading').modal('hide');
                }
            });
        }

        function mostrarModalPendente(card, item) {
            var rect = card.getBoundingClientRect();
            var scrollTop = window.pageYOffset || document.documentElement.scrollTop;
            const pendencias = item.estaPendente; // Supondo que 'estaPendente' seja uma lista de pendências
            ModalPendencia.style.top = rect.bottom + scrollTop + 'px';
            ModalPendencia.style.left = rect.left + 'px';
            ModalPendencia.style.display = 'block';

            // Limpa o conteúdo anterior dentro de DivPendencias
            DivPendencias.innerHTML = '';

            // Adiciona as pendências à DivPendencias
            pendencias.forEach(pendencia => {
                const label = document.createElement('label');
                label.textContent = pendencia;
                label.style.fontSize = '22px';
                DivPendencias.appendChild(label);
            });
        }

        function fecharModalPendente() {
            // Oculta o modal
            ModalPendencia.style.display = 'none';

            // Limpa o conteúdo dentro de DivPendencias
            DivPendencias.innerHTML = '';
        }

        // Função para aplicar os filtros
        async function aplicarFiltros(prioridade) {
            contem = $("#InputContem").val();
            ordenacao = $("input[name='opcoesOrdenacao']:checked").data("valor");

            $('input[type=checkbox][id^="checkbox"]').each(function() {
                // Verificar se o checkbox está marcado
                if ($(this).is(':checked')) {
                    // Obter o valor do checkbox
                    var colecao = $(this).val();

                    // Verificar se a coleção já existe no array
                    if (!colecoesSelecionadas.includes(colecao)) {
                        // Adicionar o valor ao array de coleções selecionadas
                        colecoesSelecionadas.push(colecao);
                    }
                }
            });
            const filtro = `${contem}/${prioridade}`

            await $('#modalLoading').modal('show');
            ConsultarOps(ApiConsulta, 1, filtro, ordenacao, colecoesSelecionadas);
        }



        async function ExportarExcel(Api, Empresa, Filtro) {
    const Dados = {
        "empresa": Empresa,
        "filtro": Filtro
    };

    try {
        const response = await fetch(`${Api}`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Authorization': Token
            },
            body: JSON.stringify(Dados),
        });

        if (response.ok) {
            const data = await response.json();
            const DetalhamentoApi = data[0]['3 -Detalhamento'];
            const colunasDesejadas = ['numeroOP', 'codProduto', 'descricao', 'Qtd Pcs', 'codFase', 'nomeFase', 'dias na Fase', 'responsavel', 'prioridade', 'justificativa', 'COLECAO', 'categoria', 'status']; // substitua com os nomes das colunas desejadas

            // Filtrar dados para incluir apenas as colunas desejadas
            const dadosFiltrados = DetalhamentoApi.map(item => {
                let novoItem = {};
                colunasDesejadas.forEach(coluna => {
                    novoItem[coluna] = item[coluna];
                });
                return novoItem;
            });

            const nomeArquivo = 'Dados Ops.xlsx';
            const wb = XLSX.utils.book_new();
            const ws = XLSX.utils.json_to_sheet(dadosFiltrados);

            // Adicionar a planilha ao workbook
            XLSX.utils.book_append_sheet(wb, ws, "Dados Op's");

            // Salvar o arquivo
            XLSX.writeFile(wb, nomeArquivo);

        } else {
            throw new Error('Erro No Retorno');
        }
    } catch (error) {
        console.error(error);
    }
}

        $('#BotaoExcel').click(() => {
            ExportarExcel(ApiConsulta, '1', $("#InputContem").val());
        })
        // Evento de clique no botão "Aplicar Filtros"
        $("#ConfirmarFiltro").click(function() {
            aplicarFiltros();
        });
