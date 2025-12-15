import requests
import os
import shutil
from datetime import date
import pathlib
from pathlib import Path
from django.conf import settings
from etl.models import ArquivoColetado

BASE_DIR = pathlib.Path(settings.BASE_DIR)
DATA_DIR = BASE_DIR / "data" / "receita_federal"
url_receita_federal = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

def baixar_arquivo_rf( # baixar receita federal
    nome_arquivo_servidor: str,
    nome_arquivo_local: str,
    ano_mes: str,
    id_arquivo,
) -> ArquivoColetado:
    arquivo_coletado = ArquivoColetado.objects.get(id=id_arquivo)
    url = os.path.join(url_receita_federal, ano_mes, nome_arquivo_servidor) # https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-07/estabelecimentos0.zip
    
    path_zip = os.path.join(DATA_DIR, ano_mes)
    os.makedirs(path_zip, exist_ok=True)
    path_zip = os.path.join(path_zip, nome_arquivo_local)
       
    re = requests.head(url)
    re.raise_for_status() 
    file_size = int(re.headers.get("Content-Length", 0))
    arquivo_coletado.expected_bytes = file_size
    
    if os.path.exists(path_zip):
        resume_byte_pos = os.path.getsize(path_zip)
        headers = {"Range": f"bytes={resume_byte_pos}-"}
        with requests.get(url, headers=headers, stream=True) as r:
            if r.status_code in (200, 206):
                mode = "ab" if resume_byte_pos > 0 else "wb"
                with open(path_zip, mode) as f:
                    for chunk in r.iter_content(chunk_size=128):
                        f.write(chunk)
    else:
        with requests.get(url, stream=True) as r:
            print(r.status_code)
            if r.status_code == 200:
                with open(path_zip, "wb") as f:
                    for chunk in r.iter_content(chunk_size=128):
                        f.write(chunk)
    
    if os.path.getsize(path_zip) == file_size:
        print("True")
        arquivo_coletado.bytes = file_size
        arquivo_coletado.status = "DOWNLOADED"
        arquivo_coletado.path_zip = path_zip
        arquivo_coletado.save()
        return arquivo_coletado
    else:
        os.remove(path_zip)
        raise RuntimeError("Download incompleto ou corrompido")

def extrair_arquivo_rf(
    arquivo_coletado: ArquivoColetado, 
) -> pathlib.Path:
    import zipfile

    extract_path = Path(arquivo_coletado.path_zip).with_suffix(".csv")  # remove .zip
    #os.makedirs(extract_path, exist_ok=True)

    with zipfile.ZipFile(arquivo_coletado.path_zip, 'r') as zip_ref:
        #zip_ref.extractall(extract_path)       
        target = None
        for info in zip_ref.infolist():
            name = info.filename
            if any(tipo in name for tipo in ['ESTABELE', 'SOCIO']):
                target = name
                break
        if not target:
            raise RuntimeError("Arquivo útil não encontrado dentro do zip")

        with zip_ref.open(target) as zin, extract_path.open("wb") as fout:
            shutil.copyfileobj(zin, fout)


    arquivo_coletado.path_extraido = str(extract_path)
    arquivo_coletado.status = "EXTRACTED"
    arquivo_coletado.save()
    return arquivo_coletado        