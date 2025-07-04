from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.contrib.auth.models import User
from rest_framework.permissions import IsAuthenticated


class RegistroView(APIView):

    def post(self, request):
        username = request.data.get('username')
        password = request.data.get('password')
        email = request.data.get('email')
        if User.objects.filter(username=username).exists():
            return Response({'error': 'El usuario ya existe.'}, status=
                status.HTTP_400_BAD_REQUEST)
        user = User.objects.create_user(username=username, password=
            password, email=email)
        return Response({'message': 'Usuario creado exitosamente.'}, status
            =status.HTTP_201_CREATED)


class UserMeView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        user = request.user
        return Response({'id': user.id, 'username': user.username, 'email':
            user.email, 'is_staff': user.is_staff})
