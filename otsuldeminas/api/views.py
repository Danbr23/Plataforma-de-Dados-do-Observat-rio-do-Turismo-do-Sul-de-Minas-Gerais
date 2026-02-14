from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import EstabelecimentoSerializer, MunicipioSerializer, SaldoMensalSerializer
from receita_federal.models import Estabelecimento
from cadastros.models import Municipio
from .services import *
from .utils import *

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

class SaldoMensalView(APIView):
    def get(self, request, codigo_ibge, data_inicio,data_fim):
        
        saldos = resgatar_saldo(codigo_ibge=codigo_ibge, data_inicio=data_inicio,data_fim=data_fim)
        #print(saldos)
        #serializer = SaldoMensalSerializer(saldos, many=True)
        return Response(saldos)

class QtdEstabelecimentos(APIView):
    
    def get(self,request):
        response = qtd_Estabelecimentos_Resumido()
        return CSVExporterResumo.export(response,"estabelecimentos.csv")

class FuncionariosPorMunicipioPorCNAE(APIView):
    
    def get(self,request):
        response = service_funcionarios_por_municipio_por_cnae()
        return CSVExporterResumo.export(response,"funcionarios.csv")

class PostosDeTrabalho(APIView):
    def get(self,request):
        response = service_postos_de_trabalho()
        return CSVExporterTemporalSaldo.export(response,"postos.csv")
    
class EstoqueAcumuladoView(APIView):
    def get(self,request):
        response = service_estoque_acumulado()
        return CSVExporterTemporalEstoque.export(response,"estoque_acumulado.csv")