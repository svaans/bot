from rest_framework.test import APITestCase
from django.contrib.auth.models import User
from .models import Tarea

class TareaTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="santi", password="1234")
        self.client.force_authenticate(user=self.user)

    def test_crear_tarea(self):
        response = self.client.post("/api/tareas/", {"titulo": "Test tarea"})
        self.assertEqual(response.status_code, 201)
