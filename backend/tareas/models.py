# tareas/models.py

from django.db import models
from django.contrib.auth.models import User

class Orden(models.Model):
    usuario = models.ForeignKey(User, on_delete=models.CASCADE, related_name='ordenes')
    symbol = models.CharField(max_length=20)
    tipo = models.CharField(max_length=10)  # "compra" o "venta"
    precio_entrada = models.FloatField()
    precio_salida = models.FloatField(null=True, blank=True)
    exito = models.BooleanField(default=False)
    fecha = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.usuario.username} - {self.symbol} - {self.tipo}"
