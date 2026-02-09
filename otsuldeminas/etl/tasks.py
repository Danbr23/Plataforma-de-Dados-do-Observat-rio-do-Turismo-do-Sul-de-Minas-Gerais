from celery import shared_task, chain, chord
from celery.exceptions import SoftTimeLimitExceeded
from requests.exceptions import Timeout, ConnectionError, RequestException, HTTPError
from datetime import date
from .services.receita_federal import baixar_arquivo_rf, extrair_arquivo_rf, filtrar_arquivo_rf, carregar_arquivo_rf
from .services.rais import baixar_rais, filtrar_vinc_pub, carregar_vinc_pub, filtrar_estab_pub, carregar_estab_pub
from .services.caged import baixar_caged, filtrar_caged, carregar_caged_mov, carregar_caged_for, carregar_caged_exc
from .models import ArquivoColetado

@shared_task(
    bind=True,
    soft_time_limit= 30 * 60,
    autoretry_for =(Timeout, ConnectionError, SoftTimeLimitExceeded),
    retry_backoff=True,   # 1s, 2s, 4s, 8s...
    retry_jitter=True,    # adiciona aleatoriedade nesses tempos
    retry_kwargs={"max_retries": 10},
)
def task_baixar_arquivo_rf(
    self,
    nome_arquivo_servidor : str,
    ano_mes: str,
    ):
            
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
        return arquivo_coletado.id
        
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
    except RuntimeError as e:
        print(e)
        raise self.retry(
            countdown= 5 * 60,  # tenta de novo em 10 minutos
            max_retries=5,           
        )
    except Exception as e:
        print(e)
        raise self.retry(
            countdown= 10 * 60,  # tenta de novo em 10 minutos
            max_retries=5,           
        )
        
@shared_task(
    bind=True,
)
def task_extrair_arquivo_rf(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        arquivoColetado = extrair_arquivo_rf(arquivoColetado)
        return arquivoColetado.id
    except RuntimeError as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise

@shared_task(
    bind=True,
)
def task_filtrar_dados_rf(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        arquivoColetado = filtrar_arquivo_rf(arquivoColetado)
        return arquivoColetado.id
    except RuntimeError as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg += "\n"
        arquivoColetado.msg += str(e)
        arquivoColetado.save()
        raise

@shared_task(
    bind=True,
)
def task_carregar_dados_rf(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        arquivoColetado = carregar_arquivo_rf(arquivoColetado)
        return arquivoColetado.id
    except RuntimeError as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg += "\n"
        arquivoColetado.msg += str(e)
        arquivoColetado.save()
        raise
    
@shared_task(
    bind=True,
)
def task_coletar_arquivo_rf(
    self,
    nome_arquivo_servidor : str,
    **kwargs,
    ):
    
    if "data" in kwargs:
        ano_mes = kwargs["data"]
    else:
        ano_mes = str(date.today())[0:7] # '2025-07'
    
    id_arquivo_coletado = task_baixar_arquivo_rf(
                            nome_arquivo_servidor=nome_arquivo_servidor,
                            ano_mes=ano_mes,
                        )
    ch = chain(
        task_extrair_arquivo_rf.si(id_arquivo_coletado),
        task_filtrar_dados_rf.s(),
        task_carregar_dados_rf.s(),
    )
    async_result = ch.apply_async()
    print(f"Coleta do arquivo {id_arquivo_coletado} disparada: {async_result.id}")
    
    
@shared_task
def task_teste(**kwargs):
    if "opa" in kwargs:
        return f"Teste OK! opa={kwargs['opa']}"
    return "Nenhum parâmetro recebido."


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
        #arquivo_coletado.status = "DOWNLOADED"
        arquivo_coletado.msg = mensagem
        arquivo_coletado.save()
        return arquivo_coletado.id
        
    except Exception as e:
        arquivo_coletado.status = "FAILED"
        arquivo_coletado.msg = str(e)
        arquivo_coletado.save()
        raise    

@shared_task(
    bind=True,
)
def task_filtrar_vinc_pub(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        filtrar_vinc_pub(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise

@shared_task(
    bind=True,
)
def task_filtrar_estab_pub(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        filtrar_estab_pub(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise
    
@shared_task(
    bind=True,
)
def task_carregar_vinc_pub(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        carregar_vinc_pub(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise

@shared_task(
    bind=True,
)
def task_carregar_estab_pub(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        carregar_estab_pub(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise

# RAIS_VINC_PUB_MG_ES_RJ.7z    
@shared_task(
    bind=True,
)
def task_coletar_vinc_pub(
    self,
    **kwargs,
    ):
    
    if "ano" in kwargs:
        ano = kwargs["ano"]
    else:
        ano = str(date.today().year)
    
    id_arquivo_coletado = task_baixar_rais(
                            ano=ano,
                            nome_arquivo_servidor="RAIS_VINC_PUB_MG_ES_RJ.7z",
                        )
    ch = chain(
        task_filtrar_vinc_pub.si(id_arquivo_coletado),
        task_carregar_vinc_pub.s(),
    )
    async_result = ch.apply_async()
    print(f"Coleta do arquivo RAIS RAIS_VINC_PUB_MG_ES_RJ.7z disparada: {async_result.id}")
    
# RAIS_ESTAB_PUB.7z   
@shared_task(
    bind=True,
)
def task_coletar_estab_pub(
    self,
    **kwargs,
    ):
    
    if "ano" in kwargs:
        ano = kwargs["ano"]
    else:
        ano = str(date.today().year)
    
    id_arquivo_coletado = task_baixar_rais(
                            ano=ano,
                            nome_arquivo_servidor="RAIS_ESTAB_PUB.7z",
                        )
    ch = chain(
        task_filtrar_estab_pub.si(id_arquivo_coletado),
        task_carregar_estab_pub.s(),
    )
    async_result = ch.apply_async()
    print(f"Coleta do arquivo RAIS RAIS_ESTAB_PUB.7z disparada: {async_result.id}")

@shared_task(
    bind=True,
)
def task_baixar_caged(
    self,
    ano: int,
    mes: int,
    nome_arquivo_servidor : str,
    ):
           
    arquivo_coletado, _ = ArquivoColetado.objects.get_or_create(
        nome=nome_arquivo_servidor,
        ano=ano,
        mes=mes,
    )
    arquivo_coletado.status = "PENDING"
    arquivo_coletado.save()
    try:
        mensagem = baixar_caged(arquivoColetado=arquivo_coletado)
        #arquivo_coletado.status = "DOWNLOADED"
        arquivo_coletado.msg = mensagem
        arquivo_coletado.save()
        return arquivo_coletado.id
        
    except Exception as e:
        arquivo_coletado.status = "FAILED"
        arquivo_coletado.msg = str(e)
        arquivo_coletado.save()
        raise

@shared_task(
    bind=True,
)
def task_filtrar_caged(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        filtrar_caged(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise

@shared_task(
    bind=True,
)
def task_carregar_caged_mov(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        carregar_caged_mov(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise
    
@shared_task(
    bind=True,
)
def task_carregar_caged_for(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        carregar_caged_for(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise

@shared_task(
    bind=True,
)
def task_carregar_caged_exc(
    self,
    id_arquivoColetado : int,
):
    arquivoColetado = ArquivoColetado.objects.get(id=id_arquivoColetado)
    try:
        carregar_caged_exc(arquivoColetado)
        return arquivoColetado.id
    except Exception as e:
        print(e)
        arquivoColetado.status = "ERROR"
        arquivoColetado.msg = str(e)
        arquivoColetado.save()
        raise
    
@shared_task(
    bind=True,
)
def task_coletar_caged_mov(
    self,
    ano: int,
    mes: int,
):
    
    id_arquivo_coletado = task_baixar_caged(
                            ano=ano,
                            mes=mes,
                            nome_arquivo_servidor=f"CAGEDMOV",
                        )
    ch = chain(
        task_filtrar_caged.si(id_arquivo_coletado),
        task_carregar_caged_mov.s(),
    )
    async_result = ch.apply_async()
    print(f"Coleta do arquivo CAGED MOV {ano}-{mes} disparada: {async_result.id}")  

@shared_task(
    bind=True,
)
def task_coletar_caged_for(
    self,
    ano: int,
    mes: int,
):
    try:
        id_arquivo_coletado = task_baixar_caged(
                                ano=ano,
                                mes=mes,
                                nome_arquivo_servidor=f"CAGEDFOR",
                            )
    except Exception as e:
        if(str(e).startswith("550")): # arquivo não encontrado
            print(f"Arquivo CAGED FOR {ano}-{mes} não encontrado. Pulando etapa de coleta.")
            return None
        else:
            raise e
    ch = chain(
        task_filtrar_caged.si(id_arquivo_coletado),
        task_carregar_caged_for.s(),
    )
    async_result = ch.apply_async()
    print(f"Coleta do arquivo CAGED FOR {ano}-{mes} disparada: {async_result.id}")

@shared_task(
    bind=True,
)
def task_coletar_caged_exc(
    self,
    ano: int,
    mes: int,
):
    try:
        id_arquivo_coletado = task_baixar_caged(
                                ano=ano,
                                mes=mes,
                                nome_arquivo_servidor=f"CAGEDEXC",
                            )
        ch = chain(
            task_filtrar_caged.si(id_arquivo_coletado),
            task_carregar_caged_exc.s(),
        )
    except Exception as e:
        if(str(e).startswith("550")): # arquivo não encontrado
            print(f"Arquivo CAGED EXC {ano}-{mes} não encontrado. Pulando etapa de coleta.")
            return None
        else:
            raise e
        
    async_result = ch.apply_async()
    print(f"Coleta do arquivo CAGED EXC {ano}-{mes} disparada: {async_result.id}")

@shared_task(
    bind=True,
)
def task_finalizar_coleta_caged(
        self,
    ):
    print("Coleta dos arquivos CAGED finalizada.")

@shared_task(
    bind=True,
)
def task_coletar_arquivos_caged(
        self,
        **kwargs,
):
    if "ano" in kwargs:
        ano = kwargs["ano"]
    else:
        ano = date.today().year
        
    if "mes" in kwargs:
        mes = kwargs["mes"]
    else:        
        mes = date.today().month
        
    ch = chain(
        task_coletar_caged_mov.si(ano=ano, mes=mes),
        task_coletar_caged_for.si(ano=ano, mes=mes),
        task_coletar_caged_exc.si(ano=ano, mes=mes),
        task_finalizar_coleta_caged.si(),
    )
    async_result = ch.apply_async()
    print(f"Coleta dos arquivos CAGED {ano}-{mes} disparada: {async_result.id}")    