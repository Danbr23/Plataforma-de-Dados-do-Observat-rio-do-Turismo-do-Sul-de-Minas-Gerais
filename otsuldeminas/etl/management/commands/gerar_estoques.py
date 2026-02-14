import csv
from django.core.management.base import BaseCommand, CommandError
from rais.models import EstoqueMensal, EstoqueAnual

class Command(BaseCommand):
    help = "sdafsdafsdaf"
 
    def handle(self, *args, **options):
        
        ano = 2021
        
        estoques_anuais = EstoqueAnual.objects.filter(referencia__year = ano)
        count=0
        for estoque in estoques_anuais:
            
            EstoqueMensal.objects.create(municipio = estoque.municipio, cnae = estoque.cnae, estoque = estoque.estoque, referencia = estoque.referencia)
            count += 1
        self.stdout.write(self.style.SUCCESS(f"Carregados {count} Estoques Mensais"))