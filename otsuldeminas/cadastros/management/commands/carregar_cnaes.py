import csv
from django.core.management.base import BaseCommand, CommandError
from cadastros.models import CNAE

class Command(BaseCommand):
    help = "Carrega data/cnae.csv para a tabela CNAE"

    def add_arguments(self, parser):
        parser.add_argument(
            "--path",
            default="arquivos_cidades_cnaes/cnae.csv",
            help="Caminho do CSV com codigo e descricao",
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
            CNAE.objects.all().delete()
        
        count = 0
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if(CNAE.objects.update_or_create(
                    codigo=str(row["codigo"]),
                    defaults={"descricao": row["descricao"]},
                )):
                
                    count += 1
        self.stdout.write(self.style.SUCCESS(f"Carregados {count} Cnaes"))