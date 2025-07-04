import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True
    dependencies = [migrations.swappable_dependency(settings.AUTH_USER_MODEL)]
    operations = [migrations.CreateModel(name='Tarea', fields=[('id',
        models.BigAutoField(auto_created=True, primary_key=True, serialize=
        False, verbose_name='ID')), ('titulo', models.CharField(max_length=
        200)), ('descripcion', models.TextField(blank=True)), ('completada',
        models.BooleanField(default=False)), ('fecha_creacion', models.
        DateTimeField(auto_now_add=True)), ('fecha_limite', models.
        DateTimeField(blank=True, null=True)), ('usuario', models.
        ForeignKey(on_delete=django.db.models.deletion.CASCADE,
        related_name='tareas', to=settings.AUTH_USER_MODEL))])]
