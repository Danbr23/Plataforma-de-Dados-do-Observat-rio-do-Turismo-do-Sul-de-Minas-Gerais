from django.db import models

# Create your models here.
class EstoqueAnual(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', to_field='codigo_ibge', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    #subclasse_cnae = models.CharField(max_length=7, default="")
    referencia = models.DateField()
    estoque = models.IntegerField(default=0)
    
    class Meta:
        verbose_name = "Estoque Anual"
        verbose_name_plural = "Estoques Anuais"
        
        indexes = [
            models.Index(fields=['municipio', 'cnae', 'referencia']),
            models.Index(fields=['municipio', 'cnae']),
        ]
    
class SaldoMensal(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', to_field='codigo_ibge', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    #subclasse_cnae = models.CharField(max_length=7, default="")
    referencia = models.DateField()
    saldo = models.IntegerField(default=0)
    
    class Meta:
        verbose_name = "Saldo Mensal"
        verbose_name_plural = "Saldos Mensais"
        indexes = [
            models.Index(fields=['municipio', 'cnae', 'referencia']),
        ]
    
class EstoqueMensal(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', to_field='codigo_ibge', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    #subclasse_cnae = models.CharField(max_length=7, default="")
    referencia = models.DateField()
    estoque = models.IntegerField(default=0)
    
    class Meta:
        verbose_name = "Estoque Mensal"
        verbose_name_plural = "Estoques Mensais"
        indexes = [
            models.Index(fields=['municipio', 'cnae', 'referencia']),
        ]