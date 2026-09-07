from urllib import response

from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.pagination import PageNumberPagination
from .serializers import EstabelecimentoSerializer, MunicipioSerializer, SaldoMensalSerializer, CNAESerializer
from receita_federal.models import Estabelecimento
from cadastros.models import Municipio, CNAE
from .services import *
from .utils import *
from drf_spectacular.utils import extend_schema, OpenApiParameter, OpenApiResponse, OpenApiExample
from drf_spectacular.types import OpenApiTypes
from rest_framework import status

# Create your views here.

class MunicipiosView(APIView):
    @extend_schema(
            summary="Listagem de Municípios",
            description="Este endpoint retorna uma lista de todos os municípios cadastrados no sistema.",
            # Definindo parâmetros de URL (ex: /api/municipios/?estado=MG)
            # parameters=[
            #     OpenApiParameter(
            #         name='estado', 
            #         description='Filtra os municípios por estado (ex: MG, SP)', 
            #         required=False, 
            #         type=OpenApiTypes.STR
            #     )
            # ],
            # Definindo o que a API retorna
            responses={
                200: MunicipioSerializer(many=True), # O Spectacular vai ler seu serializer e montar o JSON esperado!
            #    404: OpenApiResponse(description="Nenhum município encontrado para o filtro informado.")
            },
            tags=['Municípios'] # Agrupa os endpoints no menu lateral do Swagger
        )
    def get(self, request):
        municipios = Municipio.objects.all()
        serializer = MunicipioSerializer(municipios, many=True)
        return Response(serializer.data)
    
class CNAEView(APIView):
    @extend_schema(
        summary="Listagem de CNAEs",
        description="Este endpoint retorna uma lista de todas as Classificações Nacionais de Atividades Econômicas cadastradas no sistema.",
        # Definindo parâmetros de URL (ex: /api/cnaes/?setor=comercio)
        # parameters=[
        #     OpenApiParameter(
        #         name='setor', 
        #         description='Filtra os CNAEs por setor de atividade (ex: industria, comercio)', 
        #         required=False, 
        #         type=OpenApiTypes.STR
        #     )
        # ],
        # Definindo o que a API retorna
        responses={
            200: CNAESerializer(many=True), # O Spectacular vai ler seu serializer e montar o JSON esperado!
        #    404: OpenApiResponse(description="Nenhum CNAE encontrado para o filtro informado.")
        },
        tags=['CNAEs'] # Agrupa os endpoints no menu lateral do Swagger
    )
    
    def get(self, request):
        cnaes = CNAE.objects.all()
        serializer = CNAESerializer(cnaes, many=True)
        return Response(serializer.data)

class Estabelecimentos(APIView):
    
    @extend_schema(
            summary="Número de Estabelecimentos por Município",
            description='''Este endpoint retorna uma lista da quantidade de estabelecimentos por município, agrupados nas seguintes classificações: 
            Agência e Operadores, Alimentação, Comércio e Serviços, Entretenimento, Hospedagem e Transportes
            .''',
            # Definindo o que a API retorna
            responses={
                200: OpenApiResponse(
                response=OpenApiTypes.OBJECT,
                description="Retorna o dicionário aninhado com os estabelecientos por município (JSON).",
                examples=[
                    OpenApiExample(
                        name="Estrutura do JSON retornado",
                        summary="Exemplo de JSON",
                        value={
                            "Aiuruoca": {
                                "Agência e Operadores": 8,
                                "Alimentação": 107,
                                "Comércio e Serviços": 15,
                                "Entretenimento": 17,
                                "Hospedagem": 46,
                                "Transportes": 13
                            }
                        }
                    )
                ]
            ),
            },
            tags=['Estabelecimentos'] # Agrupa os endpoints no menu lateral do Swagger
        )
    
    def get(self,request):
        data = qtd_Estabelecimentos()
        formato = request.query_params.get("export", "json").lower()
        if formato == "csv":
            return CSVExporterResumo.export(data,"estabelecimentos.csv")
        return Response(data)

class Funcionarios(APIView):
    @extend_schema(
            summary="Quantidade de funcionários",
            description='''Este endpoint retorna uma lista com os valores mais recentes registrados na base de dados da quantidade de funcionários por município e por classificação:
            Agência e Operadores, Alimentação, Comércio e Serviços, Entretenimento, Hospedagem e Transportes.''',
            responses={
                200: OpenApiResponse(
                                response=OpenApiTypes.OBJECT,
                                description="Retorna o dicionário aninhado com a quantidade de funcionários.",
                                examples=[
                                    OpenApiExample(
                                        name="Estrutura do JSON retornado",
                                        summary="Exemplo de JSON",
                                        value={
                                            "ate_mes": "2026-05-31",
                                            "dados": {
                                                "Aiuruoca": {
                                                    "Comércio e Serviços": 25,
                                                    "Transportes": 0,
                                                    "Alimentação": 16,
                                                            "Entretenimento": 3,
                                                            "Agência e Operadores": 0,
                                                            "Hospedagem": 24
                                                        },
                                                        "Alagoa": {
                                                            "Comércio e Serviços": 5,
                                                            "Transportes": 5,
                                                            "Alimentação": 3,
                                                            "Entretenimento": 1,
                                                            "Agência e Operadores": 0,
                                                            "Hospedagem": 0
                                                        },
                                                    }
                                            }
                                    )
                                ]
                            ),
            },
            tags=['Funcionários'] # Agrupa os endpoints no menu lateral do Swagger
        )
    def get(self,request):
        data = funcionarios_por_municipio_por_cnae()
        formato = request.query_params.get("export", "json").lower()
        if formato == "csv":
            return CSVExporterResumo.export(data,"funcionarios.csv")
        return Response(data)

class PostosDeTrabalho(APIView):
    @extend_schema(
        summary="Postos de Trabalho",
        description='''Retorna o saldo de funcionários mensal do município ou dos municípios, agrupados pelas categorias: 
        Agência e Operadores, Alimentação, Comércio e Serviços, Entretenimento, Hospedagem e Transportes. 
        Para JSON, o `codigo_ibge` é obrigatório, e caso dejese baixar como csv, utilize também o parâmetro `export=csv`. Para baixar os dados de todas as cidades de uma vez como csv, use `export=csv` sem o codigo_ibge.''',
        parameters=[
            OpenApiParameter(name='codigo_ibge', description='Código IBGE do município (Ex: 3151800)', required=False, type=str),
            OpenApiParameter(name='export', description='Formato (json ou csv)', required=False, type=str),
        ],
        responses={
            # Documentando o código 200 de sucesso com um exemplo real
            200: OpenApiResponse(
                response=OpenApiTypes.OBJECT,
                description="Retorna o dicionário aninhado com os saldos (JSON) ou inicia o download do arquivo (CSV).",
                examples=[
                    OpenApiExample(
                        name="Estrutura do JSON retornado",
                        summary="Exemplo de JSON",
                        value={
                            "Poços de Caldas": {
                                "Comércio": {
                                    "2023": [
                                        {"mes": 1, "saldo": 150},
                                        {"mes": 2, "saldo": -25}
                                    ]
                                }
                            }
                        }
                    )
                ]
            ),
            # Documentando os erros mapeados no nosso código
            400: OpenApiResponse(
                description="Requisição inválida. Ocorre ao pedir JSON sem informar o código IBGE.",
                response=OpenApiTypes.OBJECT,
                examples=[OpenApiExample(name="Erro 400", value={"erro": "Para visualizar em JSON, informe o 'codigo_ibge'..."})]
            ),
            404: OpenApiResponse(
                description="Município não encontrado.",
                response=OpenApiTypes.OBJECT,
                examples=[OpenApiExample(name="Erro 404", value={"erro": "Nenhum município encontrado com o código IBGE X."})]
            ),
        },
        tags =['Postos de Trabalho'] # Agrupa os endpoints no menu lateral do Swagger
    )
    def get(self, request):
        codigo_ibge = request.query_params.get("codigo_ibge")
        formato = request.query_params.get("export", "json").lower()
        
        # 1. Trava de segurança: impede travamento de JSON sem IBGE
        if not codigo_ibge and formato != "csv":
            return Response(
                {"erro": "Para visualizar em JSON, informe o 'codigo_ibge'. Para baixar a base completa, utilize '?export=csv'."}, 
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # 2. Validação do 404 (Se enviou IBGE, ele TEM que existir)
        if codigo_ibge:
            # Verifica no banco se a cidade existe. Se não, já corta a execução aqui.
            # Substitua 'Municipio' pelo nome exato do seu modelo
            if not Municipio.objects.filter(codigo_ibge=codigo_ibge).exists():
                return Response(
                    {"erro": f"Nenhum município encontrado com o código IBGE {codigo_ibge}."}, 
                    status=status.HTTP_404_NOT_FOUND
                )
                
        # 3. Busca os dados (agora a função aceita tanto um código quanto None)
        data = postos_de_trabalho(codigo_ibge)
        
        # 4. Retorno dinâmico
        if formato == "csv":
            nome_arquivo = f"postos_{codigo_ibge}.csv" if codigo_ibge else "postos.csv"
            return CSVExporterTemporalSaldo.export(data, nome_arquivo)
            
        return Response(data)

class PostosDeTrabalhoPaginado(APIView):
    
    @extend_schema(
        summary="Postos de Trabalho Paginado",
        description="Retorna o saldo mensal agrupado. Os dados JSON são paginados por município para não travar o navegador.",
        parameters=[
            OpenApiParameter(name='page', description='Número da página (padrão: 1)', required=False, type=int),
            OpenApiParameter(name='export', description='Formato de exportação (json ou csv)', required=False, type=str),
            # OpenApiParameter(name='tudo', description='Ignora a paginação e retorna todo o JSON de uma vez', required=False, type=OpenApiTypes.BOOL),
        ],
        responses={
                    # Documentando o código 200 de sucesso com um exemplo real
                    200: OpenApiResponse(
                        response=OpenApiTypes.OBJECT,
                        description="Retorna o dicionário aninhado com os saldos (JSON) ou inicia o download do arquivo (CSV).",
                        examples=[
                            OpenApiExample(
                                name="Estrutura do JSON retornado",
                                summary="Exemplo de JSON",
                                value={
                                     "count": 146,
                                    "next": "http://localhost:8000/api/postos_de_trabalho_paginado/?page=4",
                                    "previous": "http://localhost:8000/api/postos_de_trabalho_paginado/?page=2",
                                    "results": {
                                        "Caldas": {
                                            "Hospedagem": {
                                                "2021": [
                                                    {
                                                        "mes": 1,
                                                        "saldo": 1
                                                    },
                                                    {
                                                        "mes": 2,
                                                        "saldo": 0
                                                    }
                                                ]
                                            }
                                        }
                                    }  
                                }
                            )
                        ]
                    ),
                    404: OpenApiResponse(
                        description="Página não encontrada.",
                        response=OpenApiTypes.OBJECT,
                        examples=[OpenApiExample(name="Erro 404", value={"detail": "Invalid page."})]
                    ),
                },
                tags =['Postos de Trabalho'] # Agrupa os endpoints no menu lateral do Swagger
    )
    def get(self, request):
        # 1. Carrega o dicionário gigante na memória
        data = postos_de_trabalho()
        
        # 2. Verifica se o usuário pediu CSV explicitamente
        formato = request.query_params.get("export", "json").lower()
        if formato == "csv":
            # Retorna o arquivo CSV (sempre completo, não faz sentido paginar CSV)
            return CSVExporterTemporalSaldo.export(data, "postos.csv")
            
        # 3. Verifica se pediu o JSON completo explicitamente
        # if request.query_params.get("tudo") == "true":
        #     return Response(data)

        # 4. PAGINAÇÃO MANUAL DO DICIONÁRIO
        paginator = PageNumberPagination()
        paginator.page_size = 1    # Retorna um município por vez (ajuste como achar melhor)
        
        # Transforma o dict em uma lista de tuplas: [('Mun A', {...}), ('Mun B', {...})]
        lista_municipios = sorted(data.items(), key=lambda item: item[0])
        
        # Passa a lista para o paginador do DRF fatiar a página atual
        pagina_atual = paginator.paginate_queryset(lista_municipios, request, view=self)
        
        # Transforma a lista fatiada (da página atual) de volta em um dicionário
        dados_paginados = dict(pagina_atual)
        
        # Retorna no padrão DRF, que inclui os links de 'next' e 'previous'
        return paginator.get_paginated_response(dados_paginados)
    
class EstoqueAcumulado(APIView):
    @extend_schema(
            summary="Estoque Acumulado",
            description='''Retorna o estoque acumulado do município ou dos municípios, agrupados pelas categorias: 
            Agência e Operadores, Alimentação, Comércio e Serviços, Entretenimento, Hospedagem e Transportes. 
            Para JSON, o `codigo_ibge` é obrigatório, e caso dejese baixar como csv, utilize também o parâmetro `export=csv`. Para baixar os dados de todas as cidades de uma vez como csv, use `export=csv` sem o codigo_ibge.''',
            parameters=[
                OpenApiParameter(name='codigo_ibge', description='Código IBGE do município (Ex: 3151800)', required=False, type=str),
                OpenApiParameter(name='export', description='Formato (json ou csv)', required=False, type=str),
            ],
            responses={
                # Documentando o código 200 de sucesso com um exemplo real
                200: OpenApiResponse(
                    response=OpenApiTypes.OBJECT,
                    description="Retorna o dicionário aninhado com os estoques (JSON) ou inicia o download do arquivo (CSV).",
                    examples=[
                        OpenApiExample(
                            name="Estrutura do JSON retornado",
                            summary="Exemplo de JSON",
                            value={
                                "Poços de Caldas": {
                                    "Comércio": {
                                        "2023": [
                                            {"mes": 1, "estoque": 150},
                                            {"mes": 2, "estoque": -25}
                                        ]
                                    }
                                }
                            }
                        )
                    ]
                ),
                # Documentando os erros mapeados no nosso código
                400: OpenApiResponse(
                    description="Requisição inválida. Ocorre ao pedir JSON sem informar o código IBGE.",
                    response=OpenApiTypes.OBJECT,
                    examples=[OpenApiExample(name="Erro 400", value={"erro": "Para visualizar em JSON, informe o 'codigo_ibge'..."})]
                ),
                404: OpenApiResponse(
                    description="Município não encontrado.",
                    response=OpenApiTypes.OBJECT,
                    examples=[OpenApiExample(name="Erro 404", value={"erro": "Nenhum município encontrado com o código IBGE X."})]
                ),
            },
            tags =['Estoque Acumulado'] # Agrupa os endpoints no menu lateral do Swagger
        )
    def get(self,request):
        
        codigo_ibge = request.query_params.get("codigo_ibge")
        formato = request.query_params.get("export", "json").lower()
        
        # 1. Trava de segurança: impede travamento de JSON sem IBGE
        if not codigo_ibge and formato != "csv":
            return Response(
                {"erro": "Para visualizar em JSON, informe o 'codigo_ibge'. Para baixar a base completa, utilize '?export=csv'."}, 
                status=status.HTTP_400_BAD_REQUEST
            )
            
        # 2. Validação do 404 (Se enviou IBGE, ele TEM que existir)
        if codigo_ibge:
            # Verifica no banco se a cidade existe. Se não, já corta a execução aqui.
            # Substitua 'Municipio' pelo nome exato do seu modelo
            if not Municipio.objects.filter(codigo_ibge=codigo_ibge).exists():
                return Response(
                    {"erro": f"Nenhum município encontrado com o código IBGE {codigo_ibge}."}, 
                    status=status.HTTP_404_NOT_FOUND
                )
                
        # 3. Busca os dados (agora a função aceita tanto um código quanto None)
        data = estoque_acumulado(codigo_ibge)
        
        # 4. Retorno dinâmico
        if formato == "csv":
            nome_arquivo = f"estoque_{codigo_ibge}.csv" if codigo_ibge else "estoques.csv"
            return CSVExporterTemporalEstoque.export(data, nome_arquivo)
            
        return Response(data)

class EstoqueAcumuladoPaginado(APIView):
    @extend_schema(
            summary="Estoque Acumulado",
            description='''"Retorna o estoque acumulado. Os dados JSON são paginados por município para não travar o navegador.''',
            parameters=[
                OpenApiParameter(name='page', description='Número da página (padrão: 1)', required=False, type=int),
                OpenApiParameter(name='export', description='Formato (json ou csv)', required=False, type=str),
            ],
            responses={
                # Documentando o código 200 de sucesso com um exemplo real
                200: OpenApiResponse(
                    response=OpenApiTypes.OBJECT,
                    description="Retorna o dicionário aninhado com os estoques (JSON) ou inicia o download do arquivo (CSV).",
                    examples=[
                        OpenApiExample(
                            name="Estrutura do JSON retornado",
                            summary="Exemplo de JSON",
                            value={
                                    "count": 146,
                                "next": "http://localhost:8000/api/postos_de_trabalho_paginado/?page=4",
                                "previous": "http://localhost:8000/api/postos_de_trabalho_paginado/?page=2",
                                "results": {
                                    "Caldas": {
                                        "Hospedagem": {
                                            "2021": [
                                                {
                                                    "mes": 1,
                                                    "estoque": 1
                                                },
                                                {
                                                    "mes": 2,
                                                    "estoque": 0
                                                }
                                            ]
                                        }
                                    }
                                }  
                            }
                        )
                    ]
                ),
                404: OpenApiResponse(
                    description="Página não encontrada.",
                    response=OpenApiTypes.OBJECT,
                    examples=[OpenApiExample(name="Erro 404", value={"detail": "Invalid page."})]
                ),
            },
            tags =['Estoque Acumulado'] # Agrupa os endpoints no menu lateral do Swagger
        )
    def get(self,request):
                
        # 3. Busca os dados (agora a função aceita tanto um código quanto None)
        data = estoque_acumulado()
        
        # 4. Retorno dinâmico
        formato = request.query_params.get("export", "json").lower()
        if formato == "csv":
            return CSVExporterTemporalEstoque.export(data, "estoques.csv")
            
        # 3. Verifica se pediu o JSON completo explicitamente
        # if request.query_params.get("tudo") == "true":
        #     return Response(data)

        # 4. PAGINAÇÃO MANUAL DO DICIONÁRIO
        paginator = PageNumberPagination()
        paginator.page_size = 1    # Retorna um município por vez (ajuste como achar melhor)
        
        # Transforma o dict em uma lista de tuplas: [('Mun A', {...}), ('Mun B', {...})]
        lista_municipios = sorted(data.items(), key=lambda item: item[0])
        
        # Passa a lista para o paginador do DRF fatiar a página atual
        pagina_atual = paginator.paginate_queryset(lista_municipios, request, view=self)
        
        # Transforma a lista fatiada (da página atual) de volta em um dicionário
        dados_paginados = dict(pagina_atual)
        
        # Retorna no padrão DRF, que inclui os links de 'next' e 'previous'
        return paginator.get_paginated_response(dados_paginados)
    

# class SaldoMensalView(APIView):
#     def get(self, request):
        
#         codigos = request.query_params.getlist("cod")
#         data_inicio = request.query_params.get("inicio")
#         data_fim = request.query_params.get("fim")
#         saldos = resgatar_saldo(codigos_ibge=codigos, data_inicio=data_inicio,data_fim=data_fim)
#         #print(saldos)
#         #serializer = SaldoMensalSerializer(saldos, many=True)
#         return Response(saldos)