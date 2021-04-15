from celery import current_task
import pandas as pd
import googleapiclient.discovery
from .celery_app import celery_app
from sqlalchemy import create_engine,DATETIME, String
import asyncpg
from .apply_text_analytic import get_data
import os
import requests
import logging
import aiohttp
import asyncio


engine = create_engine("postgresql://phong:phongbui62@localhost:5432/postgres")

async def get_analytic(data:pd.DataFrame) -> pd.DataFrame:
    async with aiohttp.ClientSession() as session:
        response = await asyncio.gather(*[get_data(str(i),text,session) for i,text in enumerate(data['Comment'])])
    return response

import asyncio
@celery_app.task(acks_late=True, name='get_list_videos', max_retries=3)
def get_videos_id(video_id: str, video_title: str, channel_name:str,next_page_token: str,
                  developer_key: str = "AIzaSyDCqHr2yTa_sgpnlQgpfYMio2tKXgFlZr4") -> dict:
    url = f'https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&pageToken={next_page_token}&videoId={video_id}&key={developer_key}&order=time&maxResults=100'
    response = requests.request("GET", url=url)
    if response.status_code == 200:
        data = response.json()
        if data.get('nextPageToken'):
            data = pd.DataFrame(data['items'])['snippet'].apply(pd.Series)['topLevelComment'].apply(pd.Series)[
                'snippet'].apply(pd.Series)
            data = data[['videoId','textOriginal','authorDisplayName','publishedAt','updatedAt']]
            data = data.rename(columns={"textOriginal":"Comment","authorDisplayName":"Author"})
            data['Video Title'] = video_title
            data['Channel Name'] = channel_name
            data['publishedAt'] = pd.to_datetime(data['publishedAt'],errors='coerce')
            data['updatedAt'] = pd.to_datetime(data['publishedAt'],errors='coerce')
            dtype = {
                     'videoId':String,'textOriginal':String,
                     'authorDisplayName':String,'publishedAt':DATETIME,'updatedAt':DATETIME
                    }
            logging.info(data)
            # data.to_sql("data_before_analytic",con=engine,if_exists='append',index=False,schema="alo")
            celery_app.send_task('get_list_videos', args=[video_id, video_title,channel_name,response.json().get('nextPageToken')])
            # loop=asyncio.get_event_loop()
            # loop.run_until_complete(get_analytic(data))
            return {"Status":f"{video_title} Success In This {next_page_token}"}
    else:
        return {video_id: video_title}
