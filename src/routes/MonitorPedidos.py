from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import MonitorPedidosOP


MonitorPedidos_routes = Blueprint('MonitorPedidos_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function


@MonitorPedidos_routes.route('/pcp/api/monitorPreFaturamento', methods=['POST'])
@token_required
def POST_MonitorPedidos():
    data = request.get_json()
    empresa = data.get('empresa', '-')
    # Parametros obrigatorios no POST
    iniVenda = data.get('iniVenda','-')
    finalVenda = data.get('finalVenda')
    FiltrodataEmissaoInicial = data.get('FiltrodataEmissaoInicial','')
    FiltrodataEmissaoFinal = data.get('FiltrodataEmissaoFinal','')


    parametroClassificacao = data.get('parametroClassificacao', 'DataPrevisao')  # Faturamento ou DataPrevisao

    tipoData = data.get('tipoData','DataEmissao') #DataEmissao x DataPrevOri

    # Parametros NAO OBRIGATORIOS NO POST (ESPECIAIS)
    # Array de tipo de nota
    tiponota = data.get('tiponota')
    # Array excluir codigo representante
    ArrayCodRepresExcluir = data.get('ArrayCodRepresExcluir','')
    # Array codrepresentante
    ArrayCodRepres = data.get('ArrayCodRepres','')
    # ARRAY NOME CLIENTE
    ArrayNomeCliente = data.get('ArrayNomeCliente','')
    #ARRAY CODIGO CLIENTE
    ArrayCodigoCliente= data.get('ArrayCodigoCliente','')

    #ARRAY CONCEITO CLIENTE
    ArrayConceitoCliente= data.get('ArrayConceitoCliente','')

    #ARRA REGIAO
    ArrayRegiao= data.get('ArrayRegiao','')


    #controle.InserindoStatus(rotina, ip, datainicio)

    monitorPedidosOP = MonitorPedidosOP.MonitorPedidosOP(empresa, iniVenda, finalVenda, tipoData, iniVenda, finalVenda, ArrayCodRepresExcluir,
                                                         ArrayCodRepres,ArrayNomeCliente, parametroClassificacao,
                                                         FiltrodataEmissaoInicial,FiltrodataEmissaoFinal)

    dados = monitorPedidosOP.gatinlho_de_disparo_monitor()
    #controle.salvarStatus(rotina, ip, datainicio)

    # Obtém os nomes das colunas
    column_names = dados.columns
    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)
    return jsonify(OP_data)



@MonitorPedidos_routes.route('/pcp/api/monitorOPs', methods=['GET'])
@token_required
def get_monitorOPs():
    dataInico = request.args.get('dataInico', '-')
    dataFim = request.args.get('dataFim')

    empresa = request.args.get('empresa','1')
    monitor = MonitorPedidosOP.MonitorPedidosOP(empresa, dataInico, dataFim, None, dataInico, dataFim, None,
                                                       None, None, None, None, None)
    dados = monitor.geracao_monitor_op()
    # Obtém os nomes das colunas
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)

    # Retorna os dados JSON para o cliente
    response = jsonify(OP_data)


    return response



@MonitorPedidos_routes.route('/pcp/api/ProdutosSemOP', methods=['POST'])
@token_required
def POST_ProdutosSemOP():
    data = request.get_json()
    dataInico = data.get('dataInico', '-')
    dataFim = data.get('dataFim', '-')
    empresa = data.get('empresa','1')

    dados = MonitorPedidosOP.MonitorPedidosOP(empresa , dataInico, dataFim,None, dataInico, dataFim,None,None,None,None,None, None).produtosSemOP_()

    # Converte o DataFrame em uma lista de dicionários
    OP_data = dados.to_dict(orient='records')

    return jsonify(OP_data)

@MonitorPedidos_routes.route('/pcp/api/Op_tam_cor', methods=['POST'])
@token_required
def post_Op_tam_cor():
    data = request.get_json()

    dataInico = data.get('dataInico')
    dataFim = data.get('dataFim')
    empresa = data.get('empresa','1')

    monitor = MonitorPedidosOP.MonitorPedidosOP(empresa, dataInico, dataFim, None, dataInico, dataFim, None,
                                                       None, None, None, None, None)
    dados = monitor.ops_tamanho_cor()

    # Obtém os nomes das colunas
    column_names = dados.columns

    # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
    OP_data = []
    for index, row in dados.iterrows():
        op_dict = {}
        for column_name in column_names:
            op_dict[column_name] = row[column_name]
        OP_data.append(op_dict)

    # Retorna os dados JSON para o cliente
    response = jsonify(OP_data)

    # Após retornar a resposta, reiniciar o app em uma nova thread
    #porta_atual = 8000  # Substitua pela porta correta que você está utilizando
    #thread = threading.Thread(target=monitor.reiniciandoAPP(), args=(porta_atual,))
    #thread.start()
    return response
