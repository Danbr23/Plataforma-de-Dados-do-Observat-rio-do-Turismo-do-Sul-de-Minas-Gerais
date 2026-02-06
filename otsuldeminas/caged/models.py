from django.db import models

# Create your models here.
class CAGED(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', to_field='codigo_ibge', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    referencia = models.DateField()
    saldo_caged = models.IntegerField(default=0)

    class Meta:
        unique_together = [("municipio", "cnae", "referencia")]
        indexes = [
            models.Index(fields=["municipio", "cnae", "referencia"]),
        ]

    def __str__(self):
        return f"{self.municipio} · {self.cnae.codigo} · {self.referencia}"
    