from django.contrib import admin
from .models import Municipio, CNAE

@admin.register(Municipio)
class MunicipioAdmin(admin.ModelAdmin):
    list_display = ("nome","codigo_ibge","codigo_receita_federal")
    list_filter = ("nome",)
    search_fields = ("nome", "codigo_ibge","codigo_receita_federal")
    ordering = ("nome",)

@admin.register(CNAE)
class CNAEAdmin(admin.ModelAdmin):
    list_display = ("descricao", "codigo", "classificacao_otmg")
    search_fields = ("codigo", "descricao", "classificacao_otmg")
    ordering = ("codigo",)
# Register your models here.
