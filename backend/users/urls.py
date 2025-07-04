from django.urls import path
from .views import RegistroView, UserMeView
urlpatterns = [path('', RegistroView.as_view(), name='register'), path(
    'me/', UserMeView.as_view(), name='user_me')]
