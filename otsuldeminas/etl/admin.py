from django.contrib import admin
from unfold.admin import ModelAdmin 
from .models import ArquivoColetado

# Register your models here.
#admin.site.unregister(ArquivoColetado)
@admin.register(ArquivoColetado)
class ArquivoColetadoAdmin(ModelAdmin):
    list_display = ("nome", "ano", "mes", "status", "linhas_filtradas", "bytes", "short_msg")
    list_filter = ("status", "ano", "mes")
    search_fields = ("url", "path", "ano", "mes")
    ordering = ("nome", "ano")
    readonly_fields = ("bytes","linhas_filtradas","msg")

    @admin.display(description="msg")
    def short_msg(self, obj):
        return (obj.msg or "")[:90]
    
    # @admin.display(description="%")
    # def pct(self, obj: ArquivoColetado):
    #     p = obj.progresso_pct()
    #     return f"{p}%" if p is not None else "-"

    @admin.display(description="bytes")
    def bytes_fmt(self, obj: ArquivoColetado):
        return f"{obj.bytes:,}".replace(",", ".")

    @admin.display(description="esperado")
    def expected_fmt(self, obj: ArquivoColetado):
        return f"{obj.expected_bytes:,}".replace(",", ".") if obj.expected_bytes else "-"

