from ftplib import FTP
from django.conf import settings
from django.db.models import F
from etl.models import ArquivoColetado
from cadastros.models import CNAE, Municipio
from rais.models import EstoqueAnual, SaldoMensal
import os
import py7zr
import pathlib
import pandas as pd
import calendar
from datetime import date
from itertools import product


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
    
    if os.path.getsize(path_zip_file_local) != tamanho:
        raise RuntimeError(f"Erro no download do arquivo {zip_file_remote} do ano {ano}: tamanho incorreto.")

    arquivoColetado.path_zip = path_zip_file_local
    arquivoColetado.bytes = os.path.getsize(path_zip_file_local)
    arquivoColetado.status = "DOWNLOADED"
    arquivoColetado.save()
    
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
    os.remove(arquivoColetado.path_zip)
    arquivoColetado.path_zip = ""
    arquivoColetado.status = "EXTRACTED"
    arquivoColetado.save()
    #os.remove(zip_file_local)

    return f"{csv_path.name}: {ano}"


def filtrar_vinc_pub(arquivoColetado : ArquivoColetado):
    
    
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
    mask = chunk[chunk.columns[20]].isin(lista_ibge) & chunk[chunk.columns[5]].str.startswith(cnaes, na=False) & chunk[chunk.columns[17]].str.isdigit() & chunk[chunk.columns[18]].str.isdigit()
    filtrado = chunk[mask]
    filtrado.to_csv(saida, mode="w", index=False, header=True, encoding="utf-8")
    
    while True:
        try:
            chunk = next(chunks)
            chunk = chunk.drop(chunk.columns[colunas_indejesadas],axis=1)
            mask = chunk[chunk.columns[20]].isin(lista_ibge) & chunk[chunk.columns[5]].str.startswith(cnaes, na=False) & chunk[chunk.columns[17]].str.isdigit() & chunk[chunk.columns[18]].str.isdigit()
            filtrado = chunk[mask]
            filtrado.to_csv(saida, mode="a", index=False, header=False, encoding="utf-8")
        except StopIteration:
    
            break
    print("filtrou")
    
    os.remove(arquivoColetado.path_extraido)
    arquivoColetado.path_extraido = ""
    arquivoColetado.status = "FILTERED"
    arquivoColetado.save()

def filtrar_estab_pub(arquivoColetado : ArquivoColetado):
    
    
    lista_ibge = list(Municipio.objects.values_list("codigo_ibge", flat=True))
     
    path_extraido = arquivoColetado.path_extraido
    ano = str(arquivoColetado.ano)
    
    chunk_size = 100000
    #primeiro = True
    saida = pathlib.Path(path_extraido).with_name(f"FILTRADO_RAIS_ESTABELECIMENTOS_{ano}.csv")
    arquivoColetado.path_filtrado = str(saida)
    arquivoColetado.save()
    
    cnaes = tuple(cnae for cnae in list(CNAE.objects.values_list("codigo", flat=True)))
    
    if arquivoColetado.ano <= 2022:
        chunks = pd.read_csv(path_extraido, encoding="latin1", chunksize=chunk_size, dtype=str, sep=';')
        # cnaes = tuple(cnae for cnae in list(CNAE.objects.values_list("codigo", flat=True))) #  Cidade.objects.values_list("nome")
    else:
        # cnaes = tuple(cnae[0:4] for cnae in list(CNAE.objects.values_list("codigo", flat=True))) #  Cidade.objects.values_list("nome")
        chunks = pd.read_csv(path_extraido, encoding="latin1", chunksize=chunk_size, dtype=str)
    
    #colunas_indejesadas = ['Bairros SP','Bairros Fortaleza','Bairros RJ', 'CNAE 95 Classe', 'Distritos SP', 'Regiões Adm DF']
    colunas_indejesadas = [0,1,2,5,16]
    
    
    chunk = next(chunks)
    
    chunk = chunk.drop(chunk.columns[colunas_indejesadas],axis=1)
    # mask = chunk["Município - Código"].isin(lista_ibge) & chunk["CNAE 2.0 Classe - Código"].isin(cnaes)
    mask = chunk[chunk.columns[10]].isin(lista_ibge) & chunk[chunk.columns[12]].str[0:5].isin(cnaes)
    filtrado = chunk[mask]
    filtrado.to_csv(saida, mode="w", index=False, header=True, encoding="utf-8")
    
    while True:
        try:
            chunk = next(chunks)
            chunk = chunk.drop(chunk.columns[colunas_indejesadas],axis=1)
            mask = chunk[chunk.columns[10]].isin(lista_ibge) & chunk[chunk.columns[12]].str[0:5].isin(cnaes)
            filtrado = chunk[mask]
            filtrado.to_csv(saida, mode="a", index=False, header=False, encoding="utf-8")
        except StopIteration:
    
            break
    print("filtrou")
    
    os.remove(arquivoColetado.path_extraido)
    arquivoColetado.path_extraido = ""
    arquivoColetado.status = "FILTERED"
    arquivoColetado.save()
    
def popular_saldo_mensal(ano):
    municipios_codigos = Municipio.objects.values_list('codigo_ibge', flat=True)
    cnaes_codigos = CNAE.objects.values_list('codigo',flat=True)
    meses = range(1,13)
    
    combinacoes = product(municipios_codigos, cnaes_codigos, meses)
    
    objetos_rais = []
    
    ultimos_dias = {
        mes: calendar.monthrange(ano,mes)[1]
        for mes in meses
    }
    
    for m_id, c_id, mes in combinacoes:
        objetos_rais.append(
            SaldoMensal(
                municipio_id = m_id,
                cnae_id = c_id,
                referencia = date(ano, mes, ultimos_dias[mes])
            )
        )
    
    SaldoMensal.objects.bulk_create(objetos_rais, batch_size=2000)

def popular_vinculos_ativos(ano):
    municipios_codigos = Municipio.objects.values_list('codigo_ibge', flat=True)
    cnaes_codigos = CNAE.objects.values_list('codigo',flat=True)
    ultimo_mes = [12]
    
    combinacoes = product(municipios_codigos, cnaes_codigos, ultimo_mes)
    
    objetos_rais = []
    
    ultimo_dia = 31
    
    for m_id, c_id, mes in combinacoes:
        objetos_rais.append(
            EstoqueAnual(
                municipio_id = m_id,
                cnae_id = c_id,
                referencia = date(ano, mes, ultimo_dia)
            )
        )
    
    EstoqueAnual.objects.bulk_create(objetos_rais, batch_size=2000)

def carregar_vinc_pub(arquivoColetado : ArquivoColetado):

    ano = arquivoColetado.ano
    popular_saldo_mensal(ano)
    
    meses = range(1,13)
    ultimos_dias = {
        mes: calendar.monthrange(ano,mes)[1]
        for mes in meses
    }
    
    path_filtrado = arquivoColetado.path_filtrado
    
    df = pd.read_csv(path_filtrado, encoding="utf-8", dtype=str)
    
    for idx, row in df.iterrows():
        municipio_ibge = row[df.columns[20]]
        cnae_codigo = row[df.columns[30]][0:5]
        mes_admissao = int(row[df.columns[17]])
        mes_desligamento = int(row[df.columns[18]])
        
        if mes_admissao != 0:
            data_referencia = date(int(ano), int(mes_admissao), ultimos_dias[mes_admissao])          
        
            try:
                municipio = Municipio.objects.get(codigo_ibge=municipio_ibge)
                cnae = CNAE.objects.get(codigo=cnae_codigo)
                
                updated = SaldoMensal.objects.filter(
                    municipio = municipio,
                    cnae = cnae,
                    referencia = data_referencia
                ).update(saldo = F("saldo") + 1)
                
            except Exception:
                print("Algo deu errado")
        
        if mes_desligamento != 0:
            data_referencia = date(int(ano), int(mes_desligamento), ultimos_dias[mes_desligamento])          
        
            try:
                municipio = Municipio.objects.get(codigo_ibge=municipio_ibge)
                cnae = CNAE.objects.get(codigo=cnae_codigo)
                
                updated = SaldoMensal.objects.filter(
                    municipio = municipio,
                    cnae = cnae,
                    referencia = data_referencia
                ).update(saldo = F("saldo") - 1)
                
            except Exception as e:
                print(cnae_codigo)
                print(e)
                raise
    
    
    arquivoColetado.status = "LOADED"
    arquivoColetado.save()
    print("carregou")

def carregar_estab_pub(arquivoColetado : ArquivoColetado):

    ano = arquivoColetado.ano
    popular_vinculos_ativos(ano)
    
    mes = 12
    ultimo_dia = 31
    data_referencia = date(ano, mes, ultimo_dia)
    path_filtrado = arquivoColetado.path_filtrado
    
    df = pd.read_csv(path_filtrado, encoding="utf-8", dtype=str)
    
    for idx, row in df.iterrows():
        municipio_ibge = row[df.columns[10]]
        cnae_codigo = row[df.columns[12]][0:5]
        qtd_vinculos_ativos = int(row[df.columns[3]])
        
        municipio = Municipio.objects.get(codigo_ibge=municipio_ibge)
        cnae = CNAE.objects.get(codigo=cnae_codigo)
        
        updated = EstoqueAnual.objects.filter(
            municipio = municipio,
            cnae = cnae,
            referencia = data_referencia
        ).update(quantidade = F("quantidade") + qtd_vinculos_ativos)
        
    arquivoColetado.status = "LOADED"
    arquivoColetado.save()
    print("carregou")
