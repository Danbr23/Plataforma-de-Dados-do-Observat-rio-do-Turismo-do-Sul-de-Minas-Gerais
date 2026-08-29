from urllib import response

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

class Estabelecimentos(APIView):
    
    def get(self,request):
        data = qtd_Estabelecimentos()
        formato = request.query_params.get("export", "json").lower()
        if formato == "csv":
            return CSVExporterResumo.export(data,"estabelecimentos.csv")
        return Response(data)

class Funcionarios(APIView):
    
    def get(self,request):
        data = funcionarios_por_municipio_por_cnae()
        formato = request.query_params.get("export", "json").lower()
        if formato == "csv":
            return CSVExporterResumo.export(data,"funcionarios.csv")
        return Response(data)

class PostosDeTrabalho(APIView):
    def get(self,request):
        data = postos_de_trabalho()
        # 2. Verifica se o usuário pediu CSV explicitamente via query param
        formato = request.query_params.get("export", "json").lower()
        
        if formato == "csv":
            # Retorna o arquivo CSV usando a sua classe existente
            return CSVExporterTemporalSaldo.export(data, "postos.csv")
            
        # 3. Padrão: retorna JSON
        return Response(data)
    
class EstoqueAcumulado(APIView):
    def get(self,request):
        data = estoque_acumulado()
        # 2. Verifica se o usuário pediu CSV explicitamente via query param
        formato = request.query_params.get("export", "json").lower()

        if formato == "csv":
            # Retorna o arquivo CSV usando a sua classe existente
            return CSVExporterTemporalEstoque.export(data, "estoque_acumulado.csv")

        # 3. Padrão: retorna JSON
        return Response(data)