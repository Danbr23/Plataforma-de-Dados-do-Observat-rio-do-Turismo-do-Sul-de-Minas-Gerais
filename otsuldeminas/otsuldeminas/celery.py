# config/celery.py
import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "otsuldeminas.settings")

app = Celery("otsuldeminas")
app.config_from_object("django.conf:settings", namespace="CELERY")
app.conf.worker_redirect_stdouts = True
app.conf.worker_redirect_stdouts_level = "INFO"
app.autodiscover_tasks()