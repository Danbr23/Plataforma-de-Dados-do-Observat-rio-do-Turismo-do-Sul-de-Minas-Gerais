from ftplib import FTP
from django.conf import settings
from etl.models import ArquivoColetado
from cadastros.models import CNAE, Municipio
import py7zr
import os
import pathlib
import pandas as pd

BASE_DIR = pathlib.Path(settings.BASE_DIR)
DATA_DIR = BASE_DIR / "data" / "rais"

def baixar_rais(arquivoColetado : ArquivoColetado) -> str:
    ano = str(arquivoColetado.ano)
    url_base = "/pdet/microdados/RAIS/"
    url_ano = ano + "/"
    url_final = url_base + url_ano
    ftp = FTP('ftp.mtps.gov.br')
    ftp.login()
    ftp.encoding = "latin-1"
    #print(ftp.dir(url_final))
    ftp.cwd(url_final)
    zip_file_remote = arquivoColetado.nome 
    tamanho = ftp.size(zip_file_remote)  # Testa se o arquivo existe no servidor
    arquivoColetado.expected_bytes = tamanho
    arquivoColetado.save()
    dir_zip_file_local = os.path.join(DATA_DIR, ano)
    os.makedirs(dir_zip_file_local, exist_ok=True)
    path_zip_file_local = os.path.join(dir_zip_file_local, zip_file_remote)
    
    with open(path_zip_file_local, "wb") as f:
        ftp.retrbinary(f"RETR {zip_file_remote}", f.write)
    
    ftp.quit()
    
    arquivoColetado.path_zip = path_zip_file_local
    arquivoColetado.bytes = os.path.getsize(path_zip_file_local)
    arquivoColetado.save()
    
    if os.path.getsize(path_zip_file_local) != tamanho:
        raise RuntimeError(f"Erro no download do arquivo {zip_file_remote} do ano {ano}: tamanho incorreto.")
    
    with py7zr.SevenZipFile(path_zip_file_local, mode='r') as z:
        nome = z.getnames()[0]
        z.extractall(path=dir_zip_file_local)

    txt_file_local = pathlib.Path(path_zip_file_local).with_name(nome)
    if zip_file_remote.startswith("RAIS_ESTAB_PUB"):
        csv_path = txt_file_local.with_name("RAIS_ESTAB_PUB.csv")
    else:
        csv_path = txt_file_local.with_name("RAIS_VINCULOS_ATIVOS.csv")    

    os.rename(txt_file_local, csv_path)
    arquivoColetado.path_extraido = str(csv_path)
    arquivoColetado.save()
    #os.remove(zip_file_local)

    return f"RAIS_ESTAB_PUB.csv: {ano}"


def filtrar_rais(arquivoColetado : ArquivoColetado):
    
    
    lista_ibge = list(Municipio.objects.values_list("codigo_ibge", flat=True))
     
    path_extraido = arquivoColetado.path_extraido
    ano = str(arquivoColetado.ano)
    
    chunk_size = 100000
    #primeiro = True
    saida = pathlib.Path(path_extraido).with_name(f"FILTRADO_RAIS_VINCULOS_ATIVOS_{ano}.csv")
    arquivoColetado.path_filtrado = str(saida)
    arquivoColetado.save()
    
    if arquivoColetado.ano <= 2022:
        chunks = pd.read_csv(path_extraido, encoding="latin1", chunksize=chunk_size, dtype=str, sep=';')
        cnaes = tuple(cnae for cnae in list(CNAE.objects.values_list("codigo", flat=True))) #  Cidade.objects.values_list("nome")
    else:
        cnaes = tuple(cnae[0:4] for cnae in list(CNAE.objects.values_list("codigo", flat=True))) #  Cidade.objects.values_list("nome")
        chunks = pd.read_csv(path_extraido, encoding="latin1", chunksize=chunk_size, dtype=str)
    
    #colunas_indejesadas = ['Bairros SP','Bairros Fortaleza','Bairros RJ', 'CNAE 95 Classe', 'Distritos SP', 'Regiões Adm DF']
    colunas_indejesadas = [0,1,2,9,10,31]
    
    
    chunk = next(chunks)
    
    chunk = chunk.drop(chunk.columns[colunas_indejesadas],axis=1)
    # mask = chunk["Município - Código"].isin(lista_ibge) & chunk["CNAE 2.0 Classe - Código"].isin(cnaes)
    mask = chunk[chunk.columns[20]].isin(lista_ibge) & chunk[chunk.columns[5]].str.startswith(cnaes, na=False)
    filtrado = chunk[mask]
    filtrado.to_csv(saida, mode="w", index=False, header=True, encoding="utf-8")
    
    while True:
        try:
            chunk = next(chunks)
            chunk = chunk.drop(chunk.columns[colunas_indejesadas],axis=1)
            mask = chunk[chunk.columns[20]].isin(lista_ibge) & chunk[chunk.columns[5]].str.startswith(cnaes, na=False)
            filtrado = chunk[mask]
            filtrado.to_csv(saida, mode="a", index=False, header=False, encoding="utf-8")
        except StopIteration:
    
            break
    print("filtrou")


    