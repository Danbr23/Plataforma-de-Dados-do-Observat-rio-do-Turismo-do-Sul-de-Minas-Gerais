from rest_framework import serializers
from receita_federal.models import Estabelecimento
from caged.models import CAGED
from cadastros.models import Municipio, CNAE
from rais.models import VinculosAtivos, Saldo

class EstabelecimentoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Estabelecimento
        fields = '__all__'

class MunicipioSerializer(serializers.ModelSerializer):
    class Meta:
        model = Municipio
        fields = '__all__'

class SaldoModelSerializer(serializers.ModelSerializer):
    class Meta:
        model = Saldo
        fields = ['referencia','saldo']
        
class SaldoSerializer(serializers.Serializer):
    mes = serializers.CharField()
    saldo = serializers.DecimalField(max_digits=18,decimal_places=2)
    
    def to_representation(self,instance):
        mes = instance["mes"]
        if hasattr(mes,"date"):
            mes = mes.date()
        return {"mes": f"{mes.year:04d}-{mes.month:02d}", "saldo": instance["saldo"]}