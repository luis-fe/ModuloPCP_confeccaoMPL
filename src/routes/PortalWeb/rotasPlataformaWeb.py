from flask import Blueprint,Flask, render_template, jsonify, request
from functools import wraps
from flask_cors import CORS
import pandas as pd



rotasPlataformaWeb = Blueprint('rotasPlataformaWeb', __name__)

@rotasPlataformaWeb.route('/Home')
def home():
    return render_template('index.html')
@rotasPlataformaWeb.route('/TelaEstrutura')
def telaEstrutura():
    return render_template('TelaEstrutura.html')
@rotasPlataformaWeb.route('/TelaPlano')
def TelaPlano():
    return render_template('TelaPlano.html')
@rotasPlataformaWeb.route('/TelaPrincipal')
def TelaPrincipal():
    return render_template('TelaPrincipal.html')
@rotasPlataformaWeb.route('/TelaUsuarios')
def TelaUsuarios():
    return render_template('TelaUsuarios.html')
@rotasPlataformaWeb.route('/TelaCurva')
def TelaCurvaABC():
    return render_template('TelaCurva.html')
@rotasPlataformaWeb.route('/TelaControleFase')
def TelaControleFase():
    return render_template('TelaControleFase.html')

@rotasPlataformaWeb.route('/TelaFaturamentoGeral.html')
def TelaFaturamentoGeral():
    return render_template('TelaFaturamentoGeral.html')

@rotasPlataformaWeb.route('/TelaFaturamentoFilial.html')
def TelaFaturamentoFilial():
    return render_template('TelaFaturamentoFilial.html')

@rotasPlataformaWeb.route('/TelaFaturamentoMatriz.html')
def TelaFaturamentoMatriz():
    return render_template('TelaFaturamentoMatriz.html')

@rotasPlataformaWeb.route('/TelaFaturamentoVarejo.html')
def TelaFaturamentoVarejo():
    return render_template('TelaFaturamentoVarejo.html')

@rotasPlataformaWeb.route('/TelaFaturamentoOutraSaidas.html')
def TelaFaturamentoOutraSaida():
    return render_template('TelaFaturamentoOutraSaidas.html')

@rotasPlataformaWeb.route('/TelaCurvaVendas')
def TelaCurvaVendas():
    return render_template('TelaCurvaVendas.html')

@rotasPlataformaWeb.route('/LeadTimeProducao')
def LeadTimeProducao():
    return render_template('LeadTime.html')