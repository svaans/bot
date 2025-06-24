"""Vistas para el control del bot."""

from __future__ import annotations

import json
from django.contrib.auth.models import User
from rest_framework import generics, permissions, status
from rest_framework.views import APIView
from rest_framework.response import Response
from .models import BotConfig, BotState
from .serializers import BotConfigSerializer, BotStateSerializer
from .bot_runner import bot_runner


class BotStatusView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        state, _ = BotState.objects.get_or_create(usuario=request.user)
        serializer = BotStateSerializer(state)
        return Response(serializer.data)


class BotStartView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        started = bot_runner.start(request.user.id)
        if not started:
            return Response({"detail": "Bot ya está en ejecución."}, status=400)
        state, _ = BotState.objects.get_or_create(usuario=request.user)
        state.activo = True
        state.save()
        return Response({"detail": "Bot iniciado."})


class BotStopView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        bot_runner.stop(request.user.id)
        BotState.objects.update_or_create(usuario=request.user, defaults={"activo": False})
        return Response({"detail": "Bot detenido."})
    
class BotModeView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        modo_real = request.data.get("modo_real")
        if modo_real is None:
            return Response({"detail": "modo_real requerido"}, status=400)
        state, _ = BotState.objects.get_or_create(usuario=request.user)
        state.modo_real = bool(modo_real)
        state.save()
        return Response({"detail": "Modo actualizado."})


class BotRestartView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        bot_runner.stop(request.user.id)
        bot_runner.start(request.user.id)
        state, _ = BotState.objects.get_or_create(usuario=request.user)
        state.activo = True
        state.save()
        return Response({"detail": "Bot reiniciado."})


class ConfigListCreateView(generics.ListCreateAPIView):
    serializer_class = BotConfigSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return BotConfig.objects.filter(usuario=self.request.user)

    def perform_create(self, serializer):
        serializer.save(usuario=self.request.user)


class ConfigDetailView(generics.RetrieveUpdateAPIView):
    serializer_class = BotConfigSerializer
    permission_classes = [permissions.IsAuthenticated]
    lookup_field = "symbol"

    def get_queryset(self):
        return BotConfig.objects.filter(usuario=self.request.user)


class ConfigUploadView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def post(self, request):
        try:
            configs = json.loads(request.body.decode("utf-8"))
        except Exception:
            return Response({"detail": "JSON inválido"}, status=status.HTTP_400_BAD_REQUEST)

        if not isinstance(configs, list):
            return Response({"detail": "Formato incorrecto"}, status=status.HTTP_400_BAD_REQUEST)

        for item in configs:
            serializer = BotConfigSerializer(data=item)
            if serializer.is_valid():
                BotConfig.objects.update_or_create(
                    usuario=request.user,
                    symbol=item.get("symbol"),
                    defaults=serializer.validated_data,
                )
        return Response({"detail": "Configuraciones actualizadas"})


class ConfigDownloadView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        configs = BotConfig.objects.filter(usuario=request.user)
        serializer = BotConfigSerializer(configs, many=True)
        return Response(serializer.data)


class MetricsView(APIView):
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request):
        ordenes = request.user.ordenes.all()
        total = len(ordenes)
        if not total:
            return Response({
                "ganancia_acumulada": 0,
                "winrate": 0,
                "operaciones": 0,
                "saldo_actual": 0,
            })
        ganancia = 0.0
        exitosas = 0
        for o in ordenes:
            if o.precio_salida is None:
                continue
            if o.tipo == "compra":
                ganancia += o.precio_salida - o.precio_entrada
            else:
                ganancia += o.precio_entrada - o.precio_salida
            if o.exito:
                exitosas += 1
        winrate = (exitosas / total) * 100 if total else 0
        return Response({
            "ganancia_acumulada": round(ganancia, 4),
            "winrate": round(winrate, 2),
            "operaciones": total,
            "saldo_actual": round(ganancia, 4),
        })