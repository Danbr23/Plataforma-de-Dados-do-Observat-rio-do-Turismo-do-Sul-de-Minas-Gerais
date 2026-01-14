from celery import shared_task, chain, chord
from celery.exceptions import SoftTimeLimitExceeded
#from requests.exceptions import Timeout, ConnectionError, RequestException, HTTPError
#from datetime import date
from .services.baixar import baixar_rais
from .models import ArquivoColetado

@shared_task(
    bind=True,
)
def task_baixar_rais(
    self,
    ano: str,
    nome_arquivo_servidor : str,
    ):
           
    arquivo_coletado, _ = ArquivoColetado.objects.get_or_create(
        nome=nome_arquivo_servidor,
        ano=int(ano),
    )
    arquivo_coletado.status = "PENDING"
    arquivo_coletado.save()
    try:
        mensagem = baixar_rais(arquivoColetado=arquivo_coletado)
        arquivo_coletado.status = "DOWNLOADED"
        arquivo_coletado.msg = mensagem
        arquivo_coletado.save()
        return arquivo_coletado.id
        
    except Exception as e:
        arquivo_coletado.status = "FAILED"
        arquivo_coletado.msg = str(e)
        arquivo_coletado.save()
        raise