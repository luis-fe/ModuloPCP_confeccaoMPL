from flask import Blueprint,Flask, render_template, jsonify, request
from functools import wraps
from flask_cors import CORS
import pandas as pd



rotasPlataformaWeb = Blueprint('rotasPlataformaWeb', __name__)

@rotasPlataformaWeb.route('/Home')
def home():
    return render_template('index.html')


@rotasPlataformaWeb.route('/templates/TelaFaturamentoGeral.html')
def TelaFaturamentoGeral():
    return render_template('TelaFaturamentoGeral.html')

@rotasPlataformaWeb.route('/templates/TelaFaturamentoFilial.html')
def TelaFaturamentoFilial():
    return render_template('TelaFaturamentoFilial.html')

@rotasPlataformaWeb.route('/templates/TelaFaturamentoMatriz.html')
def TelaFaturamentoMatriz():
    return render_template('TelaFaturamentoMatriz.html')

@rotasPlataformaWeb.route('/templates/TelaFaturamentoVarejo.html')
def TelaFaturamentoVarejo():
    return render_template('TelaFaturamentoVarejo.html')

@rotasPlataformaWeb.route('/templates/TelaFaturamentoOutraSaidas.html')
def TelaFaturamentoOutraSaida():
    return render_template('TelaFaturamentoOutraSaidas.html')

@rotasPlataformaWeb.route('/TelaCurvaVendas')
def TelaCurvaVendas():
    return render_template('TelaCurvaVendas.html')

@rotasPlataformaWeb.route('/LeadTimeProducao')
def LeadTimeProducao():
    return render_template('LeadTime.html')



@rotasPlataformaWeb.route('/templates/login.html')
def login():
    return render_template('login.html')

@rotasPlataformaWeb.route('/templates/TiposdeNotas.html')
def TiposdeNotas():
    return render_template('TiposdeNotas.html')

@rotasPlataformaWeb.route('/templates/TelaConfiguracaoMetas.html')
def TelaConfiguracaoMetas():
    return render_template('TelaConfiguracaoMetas.html')