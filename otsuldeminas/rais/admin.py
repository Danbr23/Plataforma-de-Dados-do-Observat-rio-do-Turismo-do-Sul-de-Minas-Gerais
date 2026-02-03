from django.contrib import admin
from .models import VinculosAtivos, Saldo

# Register your models here.
admin.site.register(VinculosAtivos)


@admin.register(Saldo)
class SaldoAdmin(admin.ModelAdmin):
    list_display = ('municipio', 'cnae', 'referencia', 'saldo')
    search_fields = ('referencia','cnae__codigo','municipio__nome')
    #list_filter = ('referencia','cnae','municipio')
    ordering = ('-referencia', 'municipio__nome', 'cnae__codigo')