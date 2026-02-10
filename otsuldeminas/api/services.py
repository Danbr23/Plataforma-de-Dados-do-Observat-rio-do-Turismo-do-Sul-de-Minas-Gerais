from cadastros.models import Municipio
from receita_federal.models import Estabelecimento
from django.http import Http404

def get_municipio( codigo_ibge):
    try:
        return Municipio.objects.get(codigo_ibge = codigo_ibge)
    except Municipio.DoesNotExist:
        raise Http404

def qtd_estabelecimentos(codigo_ibge):
    qtd_estabelecimentos = Estabelecimento.objects.filter(                           
        codigo_municipio_rf__codigo_ibge = codigo_ibge,
        situacao_cadastral="02"
        ).count()
    
    return qtd_estabelecimentos
    

