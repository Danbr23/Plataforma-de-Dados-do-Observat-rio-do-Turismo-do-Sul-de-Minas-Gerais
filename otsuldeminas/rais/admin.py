from django.contrib import admin
from unfold.admin import ModelAdmin 

from .models import EstoqueAnual, SaldoMensal, EstoqueMensal

# Register your models here.
#admin.site.unregister(EstoqueMensal)
@admin.register(SaldoMensal)
class SaldoMensalAdmin(ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'saldo')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    list_filter = ('municipio__nome','cnae__codigo')
    date_hierarchy = 'referencia'
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')
    
@admin.register(EstoqueAnual)
class EstoqueAnualAdmin(ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'estoque')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    list_filter = ('municipio__nome','cnae__codigo')
    date_hierarchy = 'referencia'
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')
    
@admin.register(EstoqueMensal)
class EstoqueMensalAdmin(ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'estoque')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    list_filter = ('municipio__nome','cnae__codigo')
    date_hierarchy = 'referencia'
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')