from django.contrib import admin
from django.urls import path, include
from tareas.views import OrdenesAPIView
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView
urlpatterns = [path('admin/', admin.site.urls), path('api/token/',
    TokenObtainPairView.as_view(), name='token_obtain_pair'), path(
    'api/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
    path('api/ordenes/', OrdenesAPIView.as_view(), name='ordenes'), path(
    'api/users/', include('users.urls')), path('api/bot/', include(
    'botcontrol.urls'))]
