import types
from django.conf import settings
import django
import importlib
import sys

if not settings.configured:
    importlib.import_module("backend.backend.botcontrol.apps").BotcontrolConfig.name = "backend.backend.botcontrol"
    settings.configure(
        INSTALLED_APPS=[
            'django.contrib.contenttypes',
            'django.contrib.auth',
            'rest_framework',
            'backend.backend.botcontrol',
        ],
        DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}},
        DEFAULT_AUTO_FIELD='django.db.models.AutoField',
        SECRET_KEY='x',
    )
django.setup()

# stub serializers module to avoid importing full Django models
dummy_serializers = types.ModuleType("serializers")
class _S:
    def __init__(self, data=None):
        self.data = data or {}
    def is_valid(self):
        return True
    @property
    def validated_data(self):
        return self.data

dummy_serializers.BotConfigSerializer = _S
dummy_serializers.BotStateSerializer = _S
sys.modules.setdefault("backend.backend.botcontrol.serializers", dummy_serializers)
import importlib.util
import pathlib
views_path = pathlib.Path(__file__).resolve().parents[1] / "backend" / "backend" / "botcontrol" / "views.py"
spec = importlib.util.spec_from_file_location("backend.backend.botcontrol.views", views_path)
views = importlib.util.module_from_spec(spec)
sys.modules["backend.backend.botcontrol.views"] = views
spec.loader.exec_module(views)
ConfigUploadView = views.ConfigUploadView
from rest_framework import status

class DummyReq:
    def __init__(self, body):
        self.body = body
        self.user = types.SimpleNamespace()


def test_upload_invalid_json():
    view = ConfigUploadView()
    req = DummyReq(b'{bad json')
    resp = view.post(req)
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "JSON inválido"