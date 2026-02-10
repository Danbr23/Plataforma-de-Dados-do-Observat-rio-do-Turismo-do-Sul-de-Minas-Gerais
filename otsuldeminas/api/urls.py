from rest_framework.routers import DefaultRouter
from .views import  MunicipiosView, SummaryView
from django.urls import path, include


urlpatterns = [
    path("municipios/", MunicipiosView.as_view(), name = "Códigos dos municipios"),
    path("summary/<str:codigo_ibge>/", SummaryView.as_view())
]

