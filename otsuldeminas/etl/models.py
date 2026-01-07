from django.db import models

# Create your models here.
class ArquivoColetado(models.Model):
    ARQ_STATUS = [
        ("PENDING", "Pendente"),
        ("DOWNLOADED", "Baixado"),
        ("EXTRACTED", "Extraído"),
        ("FILTERED", "Filtrado"),
        ("LOADED", "Carregado"),
        ("FAILED", "Falhou"),
    ]
    id = models.BigAutoField(primary_key=True)
    nome = models.CharField(max_length=50, default="")  # 0..9d()
    ano = models.IntegerField(default=0)
    mes = models.IntegerField(default=0)
    path_zip = models.TextField(blank=True, default="")
    path_extraido= models.TextField(blank=True, default="")
    path_filtrado= models.TextField(blank=True, default="")
    #sha256 = models.CharField(max_length=64, blank=True, default="")
    bytes = models.BigIntegerField(default=0)
    status = models.CharField(max_length=10, choices=ARQ_STATUS, default="PENDING")
    linhas_filtradas = models.BigIntegerField(default=0)
    msg = models.TextField(blank=True, default="")
    expected_bytes = models.BigIntegerField(blank=True, null=True)

    class Meta:
        #unique_together = [("coleta", "nome")]
        ordering = ["nome"]

    # def progresso_pct(self) -> float | None:
    #     if self.expected_bytes and self.bytes:
    #         return round(self.bytes / self.expected_bytes * 100, 1)
    #     return None
    def __str__(self):
        return f"{self.nome}.zip"


class Municipio(models.Model):
    codigo_ibge = models.TextField(primary_key=True, unique=True) #código IBGE (7 dígitos)
    codigo_receita_federal = models.PositiveBigIntegerField(default=0, unique=True)
    nome = models.CharField(max_length=100)
    uf = models.CharField(max_length=2, default="MG")
    class Meta:
        verbose_name = "Municipio"
        verbose_name_plural = "Municípios"  
    def __str__(self):
        return f"{self.nome}/{self.uf} ({self.codigo_ibge})"


class CNAE(models.Model):
    codigo = models.CharField(max_length=5, unique=True)  # ex.: "56112"
    descricao = models.TextField()
    classificacao_otmg = models.TextField(blank=True, null=True)
    class Meta:
        ordering = ["codigo"]
    def __str__(self):
        return f"{self.codigo} - {self.descricao[:60]}"


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
        CNAE,
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
        Municipio,
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