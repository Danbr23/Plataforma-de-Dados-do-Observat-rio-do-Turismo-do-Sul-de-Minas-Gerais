import csv
from django.core.management.base import BaseCommand, CommandError
from etl.models import Municipio

class Command(BaseCommand):
    help = "Carrega data/municipios_sul_mg.csv para a tabela municipios"

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            default="arquivos_cidades_cnaes/municipios_sul_mg.csv",
            help="Caminho do CSV com ibge, nome, uf",
        )
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Apaga e recria todos os registros",
        )
    
    def handle(self, *args, **options):
        path = options["path"]
        replace = options["replace"]
        if replace:
            Municipio.objects.all().delete()
        
        count = 0
        with open(path, newline="", encoding="utf-8") as f1:
            reader = csv.DictReader(f1)
            for row in reader:
                Municipio.objects.update_or_create(
                    codigo_ibge=int(row["ibge"]),
                    codigo_receita_federal = int(row["receita_federal"]),
                    defaults={"nome": row["nome"], "uf":row.get("uf", "MG")},
                )
                count += 1
        self.stdout.write(self.style.SUCCESS(f"Carregados {count} municípios"))