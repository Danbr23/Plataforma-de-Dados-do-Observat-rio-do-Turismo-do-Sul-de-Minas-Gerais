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
    codigo = models.CharField(max_length=5, unique=True)  # ex.: "5611200"
    descricao = models.TextField()
    classificacao_otmg = models.TextField(blank=True, null=True)

    class Meta:
        ordering = ["codigo"]

    def __str__(self):
        return f"{self.codigo} - {self.descricao[:60]}"

