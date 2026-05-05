from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from .serializers import EstabelecimentoSerializer, MunicipioSerializer, SaldoMensalSerializer, CNAESerializer
from receita_federal.models import Estabelecimento
from cadastros.models import Municipio, CNAE
from .services import *
from .utils import *

# Create your views here.

class MunicipiosView(APIView):

    def get(self, request):
        municipios = Municipio.objects.all()
        serializer = MunicipioSerializer(municipios, many=True)
        return Response(serializer.data)
    
class CNAEView(APIView):
    
    def get(self, request):
        cnaes = CNAE.objects.all()
        serializer = CNAESerializer(cnaes, many=True)
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
    def get(self, request):
        
        codigos = request.query_params.getlist("cod")
        data_inicio = request.query_params.get("inicio")
        data_fim = request.query_params.get("fim")
        saldos = resgatar_saldo(codigos_ibge=codigos, data_inicio=data_inicio,data_fim=data_fim)
        #print(saldos)
        #serializer = SaldoMensalSerializer(saldos, many=True)
        return Response(saldos)

class EstabelecimentosCSV(APIView):
    
    def get(self,request):
        response = qtd_Estabelecimentos_CSV()
        return CSVExporterResumo.export(response,"estabelecimentos.csv")

class FuncionariosCSV(APIView):
    
    def get(self,request):
        response = funcionarios_por_municipio_por_cnae_csv()
        return CSVExporterResumo.export(response,"funcionarios.csv")

class PostosDeTrabalhoCSV(APIView):
    def get(self,request):
        response = postos_de_trabalho_csv()
        return CSVExporterTemporalSaldo.export(response,"postos.csv")
    
class EstoqueAcumuladoCSV(APIView):
    def get(self,request):
        response = estoque_acumulado_csv()
        return CSVExporterTemporalEstoque.export(response,"estoque_acumulado.csv")