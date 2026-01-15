from django.db import models

class Estabelecimento(models.Model):
    cnpj_completo = models.CharField(primary_key=True, max_length=14)
    cnpj_basico = models.CharField(max_length=8)
    cnpj_ordem = models.CharField(max_length=4) # 4 dígitos seguintes do cnpj
    cnpj_dv = models.CharField(max_length=2) # 2 últimos dígitos do cnpj
    identidade_matriz_ou_filial = models.SmallIntegerField() # 1:matriz; 2:filial
    nome_fantasia = models.TextField(blank=True, null=True)
    situacao_cadastral = models.CharField(max_length=2) # 01:nula; 2:ativa; 3:suspensa; 4:inapta; 08:baxada
    data_situacao_cadastral = models.DateField(null=True, blank=True) #YYYYMMDD
    motivo_situacao_cadastral = models.SmallIntegerField(null=True, blank=True)
    nome_cidade_exterior = models.CharField(max_length=100, blank=True, null=True)
    codigo_pais = models.CharField(max_length=10, blank=True, null=True)
    data_inicio_atividade = models.DateField(null=True, blank=True) #YYYYMMDD 
    classe_cnae = models.ForeignKey(
        'cadastros.CNAE',
        to_field="codigo",
        on_delete=models.CASCADE
    )
    cnae_fiscal_principal = models.CharField(max_length=7)
    cnae_fiscal_secundaria = models.TextField(null=True, blank=True) # pode ter muitos!
    tipo_logradouro = models.CharField(max_length=30,blank=True, null=True)
    logradouro = models.TextField(null=True, blank=True)
    numero = models.CharField(max_length=10, null=True, blank=True)
    complemento = models.TextField(null=True, blank=True)
    bairro = models.TextField(blank=True, null=True)
    cep = models.CharField(max_length=8, blank=True, null=True)
    uf = models.CharField(max_length=2, blank=True, null=True)
    codigo_municipio_rf = models.ForeignKey(
        'cadastros.Municipio',
        to_field="codigo_receita_federal",   # aqui você aponta para o campo char
        on_delete=models.CASCADE,
        related_name="estabelecimentos_ibge"
    )
    dd1 = models.CharField(max_length=3, null=True, blank=True)
    telefone1 = models.CharField(max_length=15, blank=True, null=True)
    ddd2 = models.CharField(max_length=3, null=True, blank=True)
    telefone2 = models.CharField(max_length=15, blank=True, null=True)
    dddfax = models.CharField(max_length=3, null=True, blank=True)
    fax = models.CharField(max_length=15, blank=True, null=True)
    email = models.TextField(blank=True, null=True)
    situacao_especial = models.TextField(blank=True, null=True)
    data_situacao_especial = models.DateField(null=True, blank=True) #YYYY
    data_atualizacao = models.DateTimeField(auto_now=True)  # "YYYY-MM"
        
    class Meta:
        indexes = [
            models.Index(fields=["data_atualizacao"]),
            models.Index(fields=["codigo_municipio_rf"]),
            models.Index(fields=["cnpj_basico", "cnpj_ordem", "cnpj_dv"]),
        ]
        verbose_name = "Estabelecimento"
        verbose_name_plural = "Estabelecimentos"
# Create your models here.
