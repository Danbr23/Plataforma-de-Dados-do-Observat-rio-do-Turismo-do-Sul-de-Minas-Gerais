from rest_framework.routers import DefaultRouter
from .views import *
from django.urls import path, include


urlpatterns = [
    path("municipios/", MunicipiosView.as_view(), name = "Códigos dos municipios"),
    path("summary/<str:codigo_ibge>/", SummaryView.as_view()),
    path("saldo/<str:codigo_ibge>/inicio/<str:data_inicio>/fim/<str:data_fim>/", SaldoMensalView.as_view()),
    path("estabelecimentos_csv/", QtdEstabelecimentos.as_view()),
    path("funcionarios_csv/",FuncionariosPorMunicipioPorCNAE.as_view()),
    path("postos_de_trabalho_csv/",PostosDeTrabalho.as_view()),
    path("estoque_acumulado_csv/", EstoqueAcumuladoView.as_view())
]

