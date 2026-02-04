from django.contrib import admin
from .models import VinculosAtivos, Saldo

# Register your models here.

@admin.register(Saldo)
class SaldoAdmin(admin.ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'saldo')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    list_filter = ('municipio__nome','cnae__codigo')
    date_hierarchy = 'referencia'
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')
    
@admin.register(VinculosAtivos)
class VinculosAtivosAdmin(admin.ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'quantidade')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    list_filter = ('municipio__nome','cnae__codigo')
    date_hierarchy = 'referencia'
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')