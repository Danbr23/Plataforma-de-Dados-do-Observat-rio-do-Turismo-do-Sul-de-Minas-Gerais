from django.db import models

# Create your models here.
class VinculosAtivos(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', to_field='codigo_ibge', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    #subclasse_cnae = models.CharField(max_length=7, default="")
    referencia = models.DateField()
    quantidade = models.IntegerField(default=0)
    
    class Meta:
        verbose_name = "Vínculo Ativo"
        verbose_name_plural = "Vínculos Ativos"
        
        indexes = [
            models.Index(fields=['municipio', 'cnae', 'referencia']),
        ]
    
class Saldo(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', to_field='codigo_ibge', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    #subclasse_cnae = models.CharField(max_length=7, default="")
    referencia = models.DateField()
    saldo = models.IntegerField(default=0)
    
    class Meta:
        
        indexes = [
            models.Index(fields=['municipio', 'cnae', 'referencia']),
        ]
    
    
    