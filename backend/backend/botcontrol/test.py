from rest_framework.test import APITestCase
from django.contrib.auth.models import User
from botcontrol.models import BotState


class BotControlTests(APITestCase):
    def setUp(self):
        self.user = User.objects.create_user(username="user", password="pass")
        self.client.force_authenticate(user=self.user)

    def test_bot_start_stop(self):
        response = self.client.post("/api/bot/start/")
        self.assertEqual(response.status_code, 200)
        state = BotState.objects.get(usuario=self.user)
        self.assertTrue(state.activo)

        response = self.client.post("/api/bot/stop/")
        self.assertEqual(response.status_code, 200)
        state.refresh_from_db()
        self.assertFalse(state.activo)

    def test_metrics_empty(self):
        response = self.client.get("/api/bot/metricas/")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data["operaciones"], 0)