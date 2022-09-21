from rest_framework import serializers
from wrapper.models import (KafkaBrokerModel, KafkaConsumerModel, SplunkCosumerSubscribeModel)

class KafkaBrokerSerializer(serializers.ModelSerializer):
    class Meta():
        model = KafkaBrokerModel
        fields = '__all__'

class KafkaConsumerSerializer(serializers.ModelSerializer):
    class Meta():
        model = KafkaConsumerModel
        fields = '__all__'

class SplunkConsumerSunscribeSerializer(serializers.ModelSerializer):
    class Meta():
        model = SplunkCosumerSubscribeModel
        fields = '__all__'