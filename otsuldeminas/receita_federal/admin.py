from django.contrib import admin
from .models import Estabelecimento

@admin.register(Estabelecimento)
class EstabelecimentoAdmin(admin.ModelAdmin):
    list_display = ("cnpj_basico","codigo_municipio_rf", "nome_fantasia")
    list_per_page = 25
# Register your models here.
