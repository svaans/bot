from django.urls import path
from .views import OrdenesAPIView
urlpatterns = [path('ordenes/', OrdenesAPIView.as_view(), name='ordenes')]
