"""Serializers para el control del bot."""
from rest_framework import serializers
from .models import BotConfig, BotState


class BotConfigSerializer(serializers.ModelSerializer):


    class Meta:
        model = BotConfig
        fields = ['id', 'symbol', 'stop_loss', 'take_profit', 'umbral', 'pesos'
            ]


class BotStateSerializer(serializers.ModelSerializer):


    class Meta:
        model = BotState
        fields = ['activo', 'modo_real', 'actualizado']
