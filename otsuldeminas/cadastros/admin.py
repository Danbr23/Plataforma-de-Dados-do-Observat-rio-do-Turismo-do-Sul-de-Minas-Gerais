from django.contrib import admin
from unfold.admin import ModelAdmin 
from .models import Municipio, CNAE

#admin.site.unregister(Municipio)
#admin.site.unregister(CNAE)
@admin.register(Municipio)
class MunicipioAdmin(ModelAdmin):
    list_display = ("nome","codigo_ibge","codigo_receita_federal")
    list_filter = ("nome",)
    search_fields = ("nome", "codigo_ibge","codigo_receita_federal")
    ordering = ("nome",)

@admin.register(CNAE)
class CNAEAdmin(ModelAdmin):
    list_display = ("descricao", "codigo", "classificacao_otmg")
    search_fields = ("codigo", "descricao", "classificacao_otmg")
    ordering = ("codigo",)
# Register your models here.
