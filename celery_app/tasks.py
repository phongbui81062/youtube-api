from celery import Celery
import os
BROKER = os.getenv('RABBITMQ_SERVER')
BACKEND = os.getenv('REDIS_SERVER')
app = Celery('tasks', broker=BROKER,backend=BACKEND)

@app.task
def add(x, y):
    return x + y