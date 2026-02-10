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
