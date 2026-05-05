from rest_framework.routers import DefaultRouter
from .views import *
from django.urls import path, include


urlpatterns = [
    path("municipios/", MunicipiosView.as_view(), name = "Códigos dos municipios"),
    path("cnaes/", CNAEView.as_view(), name = "Informações dos cnaes"),
    path("summary/<str:codigo_ibge>/", SummaryView.as_view()),
    path("saldo/", SaldoMensalView.as_view()),
    path("estabelecimentos_csv/", EstabelecimentosCSV.as_view()),
    path("funcionarios_csv/",FuncionariosCSV.as_view()),
    path("postos_de_trabalho_csv/",PostosDeTrabalhoCSV.as_view()),
    path("estoque_acumulado_csv/", EstoqueAcumuladoCSV.as_view())
]

