from cadastros.models import Municipio
from receita_federal.models import Estabelecimento
from django.http import Http404
from django.db.models.functions import TruncMonth
from django.db.models import Sum, Max, Min
from datetime import date
import calendar
from rais.models import SaldoMensal, EstoqueAnual, EstoqueMensal
from caged.models import SaldoMensalCaged
from django.db.models import Count

def gerar_ultimo_dia(ano, mes):
    _, ultimo_dia = calendar.monthrange(ano, mes)
    return date(ano,mes,ultimo_dia)

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
    

def resgatar_saldo(codigo_ibge, data_inicio:str = None, data_fim:str = None):
    print(data_inicio)
    print(data_fim)
    if not data_fim:
        ano = date.today().year
        mes = date.today().month
        fim = gerar_ultimo_dia(ano,mes)
    else:
        ano = int(data_fim[0:4])
        mes = int(data_fim[4:6])    
        fim = gerar_ultimo_dia(ano,mes)
        
    if not data_inicio:
        inicio = date(2021,1,1)
    else:
        ano = int(data_inicio[0:4])
        mes = int(data_inicio[4:6])    
        inicio = date(ano,mes,1)
    
    if inicio > fim:
        raise
    
    print(inicio)
    print(fim)
    print(codigo_ibge)
    qs = list(SaldoMensal.objects.filter(municipio__codigo_ibge = codigo_ibge, referencia__range = (inicio,fim))
        .annotate(mes=TruncMonth("referencia"))
        .values("mes")
        .annotate(saldo=Sum("saldo"))
        .order_by("referencia")
        )
    
    #print(qs)
    ultima_data = (SaldoMensal.objects.filter(municipio__codigo_ibge = codigo_ibge).aggregate(Max("referencia"))["referencia__max"])
    print(ultima_data)
    print(fim)
    if fim > ultima_data:
        mes = ultima_data.month
        ano = ultima_data.year
        if mes == 12:
            mes = 1
            ano += 1
        else:
            mes +=1

        recomeco = date(ano,mes,1)
        print(recomeco)
        qs2 = list(SaldoMensalCaged.objects.filter(municipio__codigo_ibge = codigo_ibge, referencia__range = (recomeco,fim))
            .annotate(mes=TruncMonth("referencia"))
            .values("mes")
            .annotate(saldo=Sum("saldo_caged"))
            .order_by("referencia")
            )
        print(qs2)
        qs = qs + qs2
        
    for item in qs:
        mes = item["mes"]
        if hasattr(mes,"date"):
            mes = mes.date()
        item["mes"] = f"{mes.year:04d}-{mes.month:02d}"
        
    return qs
    
    
     
def qtd_Estabelecimentos_Resumido():
    
    #renderer_classes = [JSONRenderer, CSVRenderer]

        rows = (
            Estabelecimento.objects
            .filter(situacao_cadastral="02")
            .values(
                "codigo_municipio_rf__nome",
                "classe_cnae__classificacao_otmg"
            )
            .annotate(qtd=Count("cnpj_completo"))
            .order_by()
        )

        # Montar resposta
        out = {}
        for r in rows:
            nome = r["codigo_municipio_rf__nome"]
            classificacao = r["classe_cnae__classificacao_otmg"] or "Outros"
            out.setdefault(nome, {}).setdefault(classificacao, 0)
            out[nome][classificacao] += r["qtd"]
            
        return out

        if request.headers.get("Accept") == "text/csv":
            return CSVExporterResumo.export(out, "estabelecimentos.csv")
        return Response(out)
        
def service_funcionarios_por_municipio_por_cnae():
    
    data_mais_recente_rais = EstoqueAnual.objects.aggregate(Max("referencia"))["referencia__max"]
    data_mais_recente_caged = SaldoMensalCaged.objects.aggregate(Max("referencia"))["referencia__max"]
    
    if data_mais_recente_caged > data_mais_recente_rais:
        ate_mes = data_mais_recente_caged
        vinculos = EstoqueAnual.objects.select_related("municipio", "cnae").filter(referencia=data_mais_recente_rais)
        cageds = SaldoMensalCaged.objects.select_related("municipio","cnae").filter(referencia__gt=data_mais_recente_rais)
        data = {}
        for row in vinculos:
            nome_municipio = row.municipio.nome
            classificacao = row.cnae.classificacao_otmg or "Outros"
            total_rais = row.estoque
            
            caged_filtrado = cageds.filter(municipio = row.municipio, cnae = row.cnae)
            total_caged = caged_filtrado.aggregate(soma=Sum("saldo_caged"))["soma"]
            total = total_rais  + total_caged
            
            data.setdefault(nome_municipio, {}).setdefault(classificacao, 0)
            data[nome_municipio][classificacao] += total
            
        resposta = {
            "ate_mes": ate_mes,
            "dados": data
        }
    else:
        ate_mes = data_mais_recente_rais
        vinculos = EstoqueAnual.objects.select_related("municipio", "cnae").filter(referencia=data_mais_recente_rais)
        data = {}
        for row in vinculos:
            nome_municipio = row.municipio.nome
            classificacao = row.cnae.classificacao_otmg or "Outros"
            total_rais = row.estoque
            
            data.setdefault(nome_municipio, {}).setdefault(classificacao, 0)
            data[nome_municipio][classificacao] += total_rais
            
        resposta = {
            "ate_mes": ate_mes,
            "dados": data
        }
    
    
    return resposta

def service_postos_de_trabalho():
    data_mais_recente_rais = EstoqueAnual.objects.aggregate(Max("referencia"))["referencia__max"]
    saldos_rais = SaldoMensal.objects.select_related("municipio", "cnae").all().order_by("referencia")
    cageds = SaldoMensalCaged.objects.select_related("municipio","cnae").filter(referencia__gt=data_mais_recente_rais).order_by("referencia")
    
    agregados = {}
    for row in saldos_rais:
        nome = row.municipio.nome
        classificacao = row.cnae.classificacao_otmg or "Outros"
        ano = row.referencia.year
        mes = row.referencia.month
        
        agregados.setdefault(nome, {}).setdefault(classificacao, {}).setdefault(ano, {}).setdefault(mes, 0)
        agregados[nome][classificacao][ano][mes] += row.saldo
        
    for row in cageds:
        nome = row.municipio.nome
        classificacao = row.cnae.classificacao_otmg or "Outros"
        ano = row.referencia.year
        mes = row.referencia.month
        
        agregados.setdefault(nome, {}).setdefault(classificacao, {}).setdefault(ano, {}).setdefault(mes, 0)
        agregados[nome][classificacao][ano][mes] += row.saldo_caged
        
    data = {}
    for municipio, classes in agregados.items():
        data[municipio] = {}
        for classificacao, anos in classes.items():
            data[municipio][classificacao] = {}
            for ano, meses in anos.items():
                data[municipio][classificacao][ano] = [ {"mes": mes, "saldo": saldo} for mes, saldo in sorted(meses.items()) ]
            
    return data

def service_estoque_acumulado():
    
    estoques = EstoqueMensal.objects.select_related("municipio", "cnae").all().order_by("referencia")
    
    agregados = {}
    for row in estoques:
        nome = row.municipio.nome
        classificacao = row.cnae.classificacao_otmg or "Outros"
        ano = row.referencia.year
        mes = row.referencia.month
        
        agregados.setdefault(nome, {}).setdefault(classificacao, {}).setdefault(ano, {}).setdefault(mes, 0)
        agregados[nome][classificacao][ano][mes] += row.estoque
    
    data = {}
    for municipio, classes in agregados.items():
        data[municipio] = {}
        for classificacao, anos in classes.items():
            data[municipio][classificacao] = {}
            for ano, meses in anos.items():
                data[municipio][classificacao][ano] = [ {"mes": mes, "estoque": estoque} for mes, estoque in sorted(meses.items()) ]
    
    return data