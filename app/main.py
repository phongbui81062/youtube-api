import os
import logging
import uvicorn
from fastapi import FastAPI, BackgroundTasks

from .worker.celery_app import celery_app

log = logging.getLogger(__name__)

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/test")
def test(a:int,b:int):
    a = int(a)
    b = int(b)
    task_name = "sum"
    task = celery_app.send_task(task_name, args=[a,b])
    return {a:b}

@app.get("/videoId")
def test(url:str):
    task_name = "get_list_videos"
    task = celery_app.send_task(task_name, args=[url])
    return {"Status":"Add task susscess"}
