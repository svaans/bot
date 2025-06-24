# tareas/serializers.py

from rest_framework import serializers
from .models import Orden

class OrdenSerializer(serializers.ModelSerializer):
    class Meta:
        model = Orden
        fields = '__all__'
        read_only_fields = ['usuario', 'fecha']
