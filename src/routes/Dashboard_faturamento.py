from flask import Blueprint, jsonify, request
from functools import wraps
from src.models import Meta_Plano, Plano, Metas_Ano
import pandas as pd

dashboard_fat_routes = Blueprint('dashboard_fat_routes', __name__)

def token_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        token = request.headers.get('Authorization')
        if token == 'a44pcp22':  # Verifica se o token é igual ao token fixo
            return f(*args, **kwargs)
        return jsonify({'message': 'Acesso negado'}), 401

    return decorated_function



@dashboard_fat_routes.route('/pcp/api/dashboarTV', methods=['GET'])
def dashboarTV():
        ano = request.args.get('ano', '2025')
        empresa = request.args.get('empresa', 'Todas')

        if empresa == 'Outras':
            usuarios = PainelFaturamento.OutrosFat(ano, empresa)
            usuarios = pd.DataFrame(usuarios)
        else:
            usuarios = PainelFaturamento.Faturamento_ano(ano, empresa)
            usuarios = pd.DataFrame(usuarios)

        #os.system("clear")
        # Obtém os nomes das colunas
        column_names = usuarios.columns
        # Monta o dicionário com os cabeçalhos das colunas e os valores correspondentes
        OP_data = []
        for index, row in usuarios.iterrows():
            op_dict = {}
            for column_name in column_names:
                op_dict[column_name] = row[column_name]
            OP_data.append(op_dict)

        return jsonify(OP_data)