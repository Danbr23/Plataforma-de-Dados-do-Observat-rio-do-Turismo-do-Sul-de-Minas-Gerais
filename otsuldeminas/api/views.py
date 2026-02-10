from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import EstabelecimentoSerializer, MunicipioSerializer
from receita_federal.models import Estabelecimento
from cadastros.models import Municipio
from .services import get_municipio, qtd_estabelecimentos


# Create your views here.
# Create your views here.

class MunicipiosView(APIView):

    def get(self, request):
        municipios = Municipio.objects.all()
        serializer = MunicipioSerializer(municipios, many=True)
        return Response(serializer.data)
    
class SummaryView(APIView):

    def get(self, request, codigo_ibge):
        municipio = get_municipio(codigo_ibge)
        
        qtd_estab = qtd_estabelecimentos(codigo_ibge=codigo_ibge)
        
        response = {
            "municipio": municipio.nome,
            "estabelecimentos": qtd_estab
        }
        
        return Response(response)

    
    
        