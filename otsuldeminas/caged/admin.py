from django.contrib import admin
from unfold.admin import ModelAdmin 
from .models import SaldoMensalCaged

# Register your models here.
#admin.site.unregister(SaldoMensalCaged)
@admin.register(SaldoMensalCaged)
class SaldoMensalCagedAdmin(ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'saldo_caged')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    list_filter = ('municipio__nome','cnae__codigo')
    date_hierarchy = 'referencia'
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')