from celery import current_task
import pandas as pd
import googleapiclient.discovery
from .celery_app import celery_app
from sqlalchemy import create_engine, DATETIME, String
import asyncpg
from .apply_text_analytic import get_data
import os
import requests
import logging
import aiohttp
import asyncio


engine = create_engine(
    "postgresql://phong:phongbui62@postgressql:5432/postgres")


# @celery_app.task(acks_late=True, name='analyst_comment', max_retries=3)
# def get_analytic(i: int, text: str) -> pd.DataFrame:
#     headers = {
#         'Ocp-Apim-Subscription-Key': '102d0401e9a440ecba6e739075a5def6',
#         'Content-Type': 'application/json',
#         'Accept': 'application/json'
#     }
#     body = {
#         "documents":
#         [
#             {
#                 "language": "en",
#                 "id": i,
#                 "text": text
#             }
#         ]
#     }
#     url = "https://phongbui.cognitiveservices.azure.com/text/analytics/v3.1-preview.1/sentiment?showStats=true&opinionMining=true"
#     respones = requests.post(url, headers=headers, data=str(body))
#     respones = respones.json()
#     df = pd.DataFrame(
#         list(chain(*list(map(lambda x: x['documents'], response)))))
#     df = df['confidenceScores'].apply(pd.Series)
#     return df


@celery_app.task(acks_late=True, name='get_videos_comment', max_retries=3)
def get_videos_id(video_id: str, video_title: str, channel_name: str, next_page_token: str,
                  developer_key: str = "AIzaSyDCqHr2yTa_sgpnlQgpfYMio2tKXgFlZr4") -> dict:
    url = f'https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&pageToken={next_page_token}&videoId={video_id}&key={developer_key}&order=time&maxResults=100'
    response = requests.request("GET", url=url)
    if response.status_code == 200:
        data = response.json()
        if data.get('nextPageToken'):
            celery_app.send_task('get_videos_comment', args=[
                video_id, video_title, channel_name, response.json().get('nextPageToken')])
        data = pd.DataFrame(data['items'])['snippet'].apply(pd.Series)['topLevelComment'].apply(pd.Series)[
            'snippet'].apply(pd.Series)
        data = data[['videoId', 'textOriginal',
                     'authorDisplayName', 'publishedAt', 'updatedAt']]
        data = data.rename(
            columns={"textOriginal": "Comment", "authorDisplayName": "Author"})
        data['Video Title'] = video_title
        data['Channel Name'] = channel_name
        data['publishedAt'] = pd.to_datetime(
            data['publishedAt'], errors='coerce')
        data['updatedAt'] = pd.to_datetime(
            data['publishedAt'], errors='coerce')
        dtype = {
            'videoId': String, 'textOriginal': String,
            'authorDisplayName': String, 'publishedAt': DATETIME, 'updatedAt': DATETIME
        }
        data.to_sql("data_before_analytic",con=engine,if_exists='append',index=False,schema="alo",dtype=dtype)
        return {"Status": f"{video_title} Success In This {next_page_token}"}
    else:
        return {video_id: video_title}
