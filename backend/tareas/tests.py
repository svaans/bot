from rest_framework.test import APITestCase
from django.contrib.auth.models import User
from tareas.models import Orden 


class OrdenTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="santi", password="1234")
        self.client.force_authenticate(user=self.user)

    def test_crear_orden(self):
        data = {
            "symbol": "BTCEUR",
            "tipo": "compra",
            "precio_entrada": 10000,
        }
        response = self.client.post("/api/ordenes/", data)
        self.assertEqual(response.status_code, 201)
        self.assertEqual(Orden.objects.count(), 1)
        self.assertEqual(Orden.objects.first().symbol, "BTCEUR")
