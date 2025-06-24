"""Rutas para el control del bot."""

from django.urls import path
from . import views


urlpatterns = [
    path("status/", views.BotStatusView.as_view(), name="bot-status"),
    path("start/", views.BotStartView.as_view(), name="bot-start"),
    path("stop/", views.BotStopView.as_view(), name="bot-stop"),
    path("mode/", views.BotModeView.as_view(), name="bot-mode"),
    path("reiniciar/", views.BotRestartView.as_view(), name="bot-restart"),
    path("config/", views.ConfigListCreateView.as_view(), name="config-list"),
    path("config/<str:symbol>/", views.ConfigDetailView.as_view(), name="config-detail"),
    path("config/upload/", views.ConfigUploadView.as_view(), name="config-upload"),
    path("config/download/", views.ConfigDownloadView.as_view(), name="config-download"),
    path("metricas/", views.MetricsView.as_view(), name="metricas"),
]