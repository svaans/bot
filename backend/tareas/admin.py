from django.contrib import admin
from .models import Orden

@admin.register(Orden)
class OrdenAdmin(admin.ModelAdmin):
    list_display = ("usuario", "symbol", "tipo", "precio_entrada", "precio_salida", "exito", "fecha")
    list_filter = ("symbol", "tipo", "exito", "fecha")