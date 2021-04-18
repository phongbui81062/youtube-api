import os
import logging
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from celery import group
from .worker.celery_app import celery_app
import requests
import pandas as pd
import aiohttp
import asyncio
from itertools import chain
from .worker.apply_text_analytic import get_data
from sqlalchemy import create_engine, DATETIME, String, FLOAT
from fastapi_utils.tasks import repeat_every


log = logging.getLogger(__name__)

app = FastAPI()

engine = create_engine(
    "postgresql://phong:phongbui62@postgressql:5432/postgres")


@app.on_event("startup")
@repeat_every(seconds=35)
async def hello() -> None:
    query = '''
        SELECT * FROM alo.data_before_analytic
        limit 500
    '''
    data = pd.read_sql(query, engine)
    if len(data) != 0:
        async with aiohttp.ClientSession() as session:
            response = await asyncio.gather(*[get_data(str(i), text, session) for i, text in enumerate(data['Comment'])])
        df = pd.DataFrame(
            list(chain(*list(map(lambda x: x['documents'], response)))))
        df = df['confidenceScores'].apply(pd.Series)
        df_final = data.merge(df, left_index=True,
                              right_index=True, how='inner')
        tuple_key_to_delete = tuple(df_final['key_gen'])
        df_final = df_final.loc[:, df_final.columns != 'key_gen']
        df_final.to_sql("data_analysted", con=engine,
                        if_exists='append', index=False, schema="alo")
        if len(tuple_key_to_delete) == 1:
            query_string = f'''
                DELETE FROM alo.data_before_analytic
                WHERE key_gen = {tuple_key_to_delete[0]}
            '''
        else:
            query_string = f'''
                DELETE FROM alo.data_before_analytic
                WHERE key_gen IN {tuple_key_to_delete}
            '''
        with engine.connect() as con:
            con.execute(query_string)


@app.get("/apply_analytic")
async def analytic():
    query = '''
        SELECT * FROM alo.data_before_analytic
    '''
    data = pd.read_sql(query, engine)
    async with aiohttp.ClientSession() as session:
        response = await asyncio.gather(*[get_data(str(i), text, session) for i, text in enumerate(data['Comment'])])
    df = pd.DataFrame(
        list(chain(*list(map(lambda x: x['documents'], response)))))
    df = df['confidenceScores'].apply(pd.Series)
    df_final = data.merge(df, left_index=True, right_index=True, how='inner')
    df_final.to_sql("data_analysted", con=engine,
                    if_exists='append', index=False, schema="alo")
    return {"Success": "alooo"}


@app.get("/videoId")
def test(url: str, channel_name: str, developer_key: str = "AIzaSyDCqHr2yTa_sgpnlQgpfYMio2tKXgFlZr4"):
    channel_id = url.split('/')[-1]

    url_channel = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&order=viewCount&type=video&key={developer_key}&alt=json"
    # Get credentials and create an API client
    response = requests.request("GET", url_channel)
    if response.status_code == 200:
        response = response.json()
        # list of video_id
        video_ids = list(pd.DataFrame(response['items'])[
                         'id'].apply(pd.Series)['videoId'])
        # list of title of video
        video_titles = list(pd.DataFrame(response['items'])[
                            'snippet'].apply(pd.Series)['title'])
        # dict of video title and video id
        task_name = "get_videos_comment"
        videos = zip(video_ids, video_titles)
        group(celery_app.send_task(task_name, args=[
            vid_id, vid_title, channel_name, '']) for vid_id, vid_title in videos)()
        return {"Error": "Wrong Channel Name or Limit Requests Today"}
    else:
        return {"Error": "Wrong Channel Name or Limit Requests Today"}
