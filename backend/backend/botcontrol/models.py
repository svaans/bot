from django.db import models
from django.contrib.auth.models import User
from django.core.validators import MinValueValidator


class BotConfig(models.Model):
    """Configuración personalizada por símbolo para cada usuario."""

    usuario = models.ForeignKey(User, on_delete=models.CASCADE, related_name="configs")
    symbol = models.CharField(max_length=20)
    stop_loss = models.FloatField(default=0.0, validators=[MinValueValidator(0.0)])
    take_profit = models.FloatField(default=0.0, validators=[MinValueValidator(0.0)])
    umbral = models.FloatField(default=0.0, validators=[MinValueValidator(0.0)])
    pesos = models.JSONField(default=dict, blank=True)

    class Meta:
        unique_together = ("usuario", "symbol")


class BotState(models.Model):
    """Estado actual del bot para un usuario."""

    usuario = models.OneToOneField(User, on_delete=models.CASCADE, related_name="bot_state")
    activo = models.BooleanField(default=False)
    modo_real = models.BooleanField(default=False)
    actualizado = models.DateTimeField(auto_now=True)