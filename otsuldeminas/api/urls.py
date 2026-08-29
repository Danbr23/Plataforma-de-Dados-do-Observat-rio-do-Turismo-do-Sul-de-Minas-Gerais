from rest_framework.routers import DefaultRouter
from .views import *
from django.urls import path, include


urlpatterns = [
    path("municipios/", MunicipiosView.as_view(), name = "Códigos dos municipios"),
    path("cnaes/", CNAEView.as_view(), name = "Informações dos cnaes"),
    path("summary/<str:codigo_ibge>/", SummaryView.as_view()),
    path("saldo/", SaldoMensalView.as_view()),
    path("estabelecimentos/", Estabelecimentos.as_view()),
    path("funcionarios/",Funcionarios.as_view()),
    path("postos_de_trabalho/",PostosDeTrabalho.as_view()),
    path("estoque_acumulado/", EstoqueAcumulado.as_view())
]

