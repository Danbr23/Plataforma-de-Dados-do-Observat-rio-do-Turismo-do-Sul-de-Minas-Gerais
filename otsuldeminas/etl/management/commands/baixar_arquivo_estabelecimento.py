import csv
from django.core.management.base import BaseCommand, CommandError
from etl.models import ArquivoColetado
from etl.services.receita_federal import baixar_arquivo_rf

class Command(BaseCommand):
    help = "Baixa arquivos de estabelecimentos da receita federal"

    def add_arguments(self, parser):
        parser.add_argument(
            "--nome",
            default="Estabelecimentos0.zip",
            help="Nome do arquivo a ser baixado",
        )
        parser.add_argument(
            "--data",
            default="2020-01",
            help="Data do arquivo a ser baixado no formato AAAA-MM",
        )        
    
    def handle(self, *args, **options):
        nome_arquivo_servidor = options["nome"]
        ano_mes = options["data"]
        nome_arquivo_local = (
            nome_arquivo_servidor[: len(nome_arquivo_servidor) - 4] 
            + "_" 
            + ano_mes 
            + ".zip"
        )
        arquivo_coletado, _ = ArquivoColetado.objects.get_or_create(
            nome=nome_arquivo_local,
            ano=int(ano_mes[:4]),
            mes=int(ano_mes[5:7])
        )
        arquivo_coletado.status = "PENDING"
        arquivo_coletado.save()
        try:
            arquivo_coletado = baixar_arquivo_rf(
                                    nome_arquivo_servidor=nome_arquivo_servidor,
                                    nome_arquivo_local=nome_arquivo_local,
                                    ano_mes=ano_mes,
                                    arquivo_coletado=arquivo_coletado
                                )
        except Exception as e:
            raise CommandError(f"Erro ao baixar o arquivo: {e}")
        
        self.stdout.write(self.style.SUCCESS(f"Arquivo {arquivo_coletado.nome} baixado com sucesso."))