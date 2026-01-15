# from ftplib import FTP
# from django.conf import settings
# from rais.models import ArquivoColetado
# import py7zr
# import os
# import pathlib

# BASE_DIR = pathlib.Path(settings.BASE_DIR)
# DATA_DIR = BASE_DIR / "data" / "rais"

# def baixar_rais(arquivoColetado) -> str:
#     ano = str(arquivoColetado.ano)
#     url_base = "/pdet/microdados/RAIS/"
#     url_ano = ano + "/"
#     url_final = url_base + url_ano
#     ftp = FTP('ftp.mtps.gov.br')
#     ftp.login()
#     ftp.encoding = "latin-1"
#     #print(ftp.dir(url_final))
#     ftp.cwd(url_final)
#     zip_file_remote = arquivoColetado.nome 
#     tamanho = ftp.size(zip_file_remote)  # Testa se o arquivo existe no servidor
#     arquivoColetado.expected_bytes = tamanho
#     arquivoColetado.save()
#     dir_zip_file_local = os.path.join(DATA_DIR, ano)
#     os.makedirs(dir_zip_file_local, exist_ok=True)
#     path_zip_file_local = os.path.join(dir_zip_file_local, zip_file_remote)
    
#     with open(path_zip_file_local, "wb") as f:
#         ftp.retrbinary(f"RETR {zip_file_remote}", f.write)
    
#     ftp.quit()
    
#     arquivoColetado.path_zip = path_zip_file_local
#     arquivoColetado.bytes = os.path.getsize(path_zip_file_local)
#     arquivoColetado.save()
    
#     if os.path.getsize(path_zip_file_local) != tamanho:
#         raise RuntimeError(f"Erro no download do arquivo {zip_file_remote} do ano {ano}: tamanho incorreto.")
    
#     with py7zr.SevenZipFile(path_zip_file_local, mode='r') as z:
#         nome = z.getnames()[0]
#         z.extractall(path=dir_zip_file_local)

#     txt_file_local = pathlib.Path(path_zip_file_local).with_name(nome)
#     if zip_file_remote.startswith("RAIS_ESTAB_PUB"):
#         csv_path = txt_file_local.with_name("RAIS_ESTAB_PUB.csv")
#     else:
#         csv_path = txt_file_local.with_name("RAIS_VINCULOS_ATIVOS.csv")    

#     os.rename(txt_file_local, csv_path)
#     arquivoColetado.path_extraido = str(csv_path)
#     arquivoColetado.save()
#     #os.remove(zip_file_local)

#     return f"RAIS_ESTAB_PUB.csv: {ano}"