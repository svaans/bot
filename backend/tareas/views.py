# tareas/views.py

from rest_framework import generics, permissions
from .models import Orden
from .serializers import OrdenSerializer

class OrdenesAPIView(generics.ListCreateAPIView):
    serializer_class = OrdenSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return Orden.objects.filter(usuario=self.request.user).order_by('-fecha')

    def perform_create(self, serializer):
        serializer.save(usuario=self.request.user)
