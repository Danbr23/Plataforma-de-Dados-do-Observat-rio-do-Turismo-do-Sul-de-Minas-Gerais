import requests
import os
import charset_normalizer
import shutil
import csv
from datetime import date
import pathlib
from pathlib import Path
from django.conf import settings
from etl.models import CNAE, ArquivoColetado, Municipio
from etl.consts import SELECT_ORDER, IDX

BASE_DIR = pathlib.Path(settings.BASE_DIR)
DATA_DIR = BASE_DIR / "data" / "receita_federal"
url_receita_federal = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"

def _lista_codigo_municipios_rf() -> set[str]:
    # cache leve por processo
    if not hasattr(_lista_codigo_municipios_rf, "_cache"):
        _lista_codigo_municipios_rf._cache = set(Municipio.objects.values_list("codigo_receita_federal", flat=True))
    return _lista_codigo_municipios_rf._cache

def _lista_codigos_cnae() -> set[str]:
    if not hasattr(_lista_codigos_cnae, "_cache"):
        _lista_codigos_cnae._cache = set(CNAE.objects.values_list("codigo", flat=True))
    return _lista_codigos_cnae._cache


def baixar_arquivo_rf( # baixar receita federal
    nome_arquivo_servidor: str,
    nome_arquivo_local: str,
    ano_mes: str,
    arquivo_coletado: ArquivoColetado,
) -> ArquivoColetado:
    url = os.path.join(url_receita_federal, ano_mes, nome_arquivo_servidor) # https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/2025-07/Estabelecimentos0.zip
    
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

def filtrar_arquivo_rf(
    arquivo_coletado: ArquivoColetado,
) -> ArquivoColetado:
    extracted_path = arquivo_coletado.path_extraido
    
    if arquivo_coletado.nome.startswith("Estabelecimentos"):
        lista_codigo_municipios_rf = _lista_codigo_municipios_rf()
        lista_codigos_cnae = _lista_codigos_cnae()
        col_idx = IDX["Estabelecimentos"] 
        filtered_path = Path(extracted_path).with_name(f"filtrado_{Path(extracted_path).name}")
        total_out = 0
        #detectar encoding do arquivo extraído
        with open(extracted_path, "rb") as rawdata:
            result = charset_normalizer.detect(rawdata.read(1000000))
            encoding = result['encoding'] or "latin-1"
            print(encoding)

        with open(extracted_path, "r", encoding=encoding, newline="") as fin, filtered_path.open("w", encoding="utf-8", newline="") as fout:
            reader = csv.reader(fin, delimiter=";")
            writer = csv.writer(fout, delimiter=";")
            # header do bronze filtrado
            writer.writerow(SELECT_ORDER[0])
            for row in reader:
                # alguns dumps vêm SEM header; se detectar header, pule
                if row and row[0].upper().startswith("CNPJ"):
                    continue
                try:
                    mun = int(row[col_idx["codigo_municipio_rf"]])
                    cnae = str(row[col_idx["cnae_fiscal_principal"]])[:5]
                    #if total_out % 1000000 == 0:
                        #print(cnae)
                except Exception:
                    continue
                if mun in lista_codigo_municipios_rf and cnae in lista_codigos_cnae:
                    #print("Esta")
#                    municipio = Municipio.objects.get(codigo_receita_federal = mun)
                    out_row = [
                        (str(row[0]) + str(row[1]) + str(row[2])) if col == "cnpj_completo" 
                        else row[col_idx[col]]
                        for col in SELECT_ORDER[0]
                    ]

                    #out_row = [row[col_idx[col]] if col != "mes" else mes for col in sel]

                    writer.writerow(out_row)
                    total_out += 1

        arquivo_coletado.path_filtrado = str(filtered_path)
        arquivo_coletado.linhas_filtradas = total_out
        arquivo_coletado.status = "FILTERED"
        arquivo_coletado.save(update_fields=["path_filtrado", "linhas_filtradas", "status"])
        return arquivo_coletado
    
