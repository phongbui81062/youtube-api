import os

from celery import Celery

celery_app = Celery(
        "worker",
        backend='redis://redis:6379/0',
        broker='pyamqp://phong:phongbui62@rabbitmq:5672'
    )
    
celery_app.conf.update(task_track_started=True)