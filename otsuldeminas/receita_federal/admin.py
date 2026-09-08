from django.contrib import admin
from unfold.admin import ModelAdmin 
from .models import Estabelecimento

#admin.site.unregister(Estabelecimento)
@admin.register(Estabelecimento)
class EstabelecimentoAdmin(ModelAdmin):
    list_display = ("cnpj_basico","codigo_municipio_rf", "nome_fantasia")
    list_per_page = 25
# Register your models here.
