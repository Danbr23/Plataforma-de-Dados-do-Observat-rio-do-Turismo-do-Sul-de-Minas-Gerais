from django.db import models

# Create your models here.
class VinculosAtivos(models.Model):
    municipio=  models.ForeignKey('cadastros.Municipio', on_delete=models.CASCADE)
    cnae = models.ForeignKey('cadastros.CNAE', to_field='codigo', on_delete=models.CASCADE)
    subclasse_cnae = models.CharField(max_length=7)
    data = models.DateField()
    quantidade = models.IntegerField(default=0)
    
    