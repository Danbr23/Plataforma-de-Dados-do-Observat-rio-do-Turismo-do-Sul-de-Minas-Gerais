from celery import shared_task
from celery.exceptions import SoftTimeLimitExceeded
from requests.exceptions import Timeout, ConnectionError, RequestException, HTTPError
from datetime import date
from .services.receita_federal import baixar_arquivo_rf
from .models import ArquivoColetado



@shared_task(
    bind=True,
    soft_time_limit= 30 * 60,
    autoretry_for =(Timeout, ConnectionError, SoftTimeLimitExceeded),
    retry_backoff=True,   # 1s, 2s, 4s, 8s...
    retry_jitter=True,    # adiciona aleatoriedade nesses tempos
    retry_kwargs={"max_retries": 10},
)
def task_coletar_arquivo_rf(
    self,
    nome_arquivo_servidor : str,
    ):
    ano_mes = str(date.today())[0:7] # '2025-07'
    nome_arquivo_local = nome_arquivo_servidor[:len(nome_arquivo_servidor)-4] + "_" + ano_mes + ".zip"
    arquivo_coletado, _ = ArquivoColetado.objects.get_or_create(nome=nome_arquivo_local, ano=int(ano_mes[:4]), mes=int(ano_mes[5:7]))
    arquivo_coletado.status = "PENDING"
    arquivo_coletado.save()
    try:
        baixar_arquivo_rf(nome_arquivo_servidor=nome_arquivo_servidor, nome_arquivo_local=nome_arquivo_local, ano_mes=ano_mes, id_arquivo=arquivo_coletado.id)
        
    except (Timeout, ConnectionError, SoftTimeLimitExceeded):
        # Essas exceções são tratadas pelo autoretry_for automaticamente
        raise

    except HTTPError as e:
        if e.response.status_code == 404: #a rquivo não encontrado
            print("Ih rapaiz")
            raise self.retry(
            countdown= 7 * 24 * 60 * 60,  # tenta de novo em uma semana
            max_retries=4,           # tenta por um mes
        ) 
        