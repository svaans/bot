from django.contrib import admin
from .models import BotConfig, BotState


@admin.register(BotConfig)
class BotConfigAdmin(admin.ModelAdmin):
    list_display = ("usuario", "symbol", "stop_loss", "take_profit", "umbral")


@admin.register(BotState)
class BotStateAdmin(admin.ModelAdmin):
    list_display = ("usuario", "activo", "modo_real", "actualizado")