from flask import Blueprint
# Crie um Blueprint para as rotas

routes_blueprint = Blueprint('routes', __name__)




# Informando de onde quero importar as rotas
from src.routes.PlanoRoute import plano_routes
from src.routes.Plano_Lote import planoLote_routes
from src.routes.Pedidos_Plano import pedidosPlano_routes
from src.routes.Meta_Plano import metaPlano_routes
from src.routes.Tendencia_Plano import Tendencia_Plano_routes
from src.routes.Parametrizacao_ABC import parametrizacaoABC_routes
from src.routes.Tendencia_Plano_Materiais import Tendencia_Plano_Materiais_routes
from src.routes.Simulacao_Prog import Simulacao_prod_routes
from src.routes.Produtos import produtos_routes
from src.routes.OrdemProd import OrdemProd_routes
from src.routes.CronogramaAtividade import CronogramaAtividades_routes
from src.routes.MonitorPedidos import MonitorPedidos_routes
from  src.routes.PortalWeb.rotasPlataformaWeb import rotasPlataformaWeb
from src.routes.Meta_ano import metaAno_routes
from src.routes.Dashboard_faturamento import dashboard_fat_routes


# Importacao das rotas para o blueprint:
routes_blueprint.register_blueprint(rotasPlataformaWeb)
routes_blueprint.register_blueprint(plano_routes)
routes_blueprint.register_blueprint(planoLote_routes)
routes_blueprint.register_blueprint(pedidosPlano_routes)
routes_blueprint.register_blueprint(metaPlano_routes)
routes_blueprint.register_blueprint(Tendencia_Plano_routes)
routes_blueprint.register_blueprint(parametrizacaoABC_routes)
routes_blueprint.register_blueprint(Tendencia_Plano_Materiais_routes)
routes_blueprint.register_blueprint(Simulacao_prod_routes)
routes_blueprint.register_blueprint(produtos_routes)
routes_blueprint.register_blueprint(OrdemProd_routes)
routes_blueprint.register_blueprint(CronogramaAtividades_routes)
routes_blueprint.register_blueprint(MonitorPedidos_routes)
routes_blueprint.register_blueprint(metaAno_routes)
routes_blueprint.register_blueprint(dashboard_fat_routes)
