from flask import Blueprint
# Crie um Blueprint para as rotas

routes_blueprint = Blueprint('routes', __name__)




# Informando de onde quero importar as rotas
from src.routes.PlanoRoute import plano_routes
from src.routes.Plano_Lote import planoLote_routes


# Importacao das rotas para o blueprint:
routes_blueprint.register_blueprint(plano_routes)
routes_blueprint.register_blueprint(planoLote_routes)
