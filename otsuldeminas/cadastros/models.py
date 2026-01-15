from django.db import models

# Create your models here.
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