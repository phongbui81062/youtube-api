from celery import current_task
import pandas as pd
import googleapiclient.discovery
from .celery_app import celery_app
import os
import requests

@celery_app.task(acks_late=True,name='sum')
def test_celery(a: int, b: int) -> int:

    return f"test task return {a+b}"


@celery_app.task(acks_late=True,name='get_list_videos',max_retries=3)
def get_videos_id(url: str,api_service_name:str="youtube",
                api_version:str="v3",
                developer_key:str="AIzaSyDCqHr2yTa_sgpnlQgpfYMio2tKXgFlZr4") -> list:
    channel_id = url.split('/')[-1]

    url_channel = f"https://youtube.googleapis.com/youtube/v3/search?part=snippet&channelId={channel_id}&order=viewCount&type=video&key={developer_key}&alt=json"
    # Get credentials and create an API client
    response = requests.request("GET", url_channel)
    if response.status_code == 200:
        response = response.json()
        # list of video_id
        video_id = list(pd.DataFrame(response['items'])['id'].apply(pd.Series)['videoId'])
        # list of title of video
        video_title = list(pd.DataFrame(response['items'])['snippet'].apply(pd.Series)['title'])
        # dict of video title and video id
        return video_id
    else:
        return {"Error":"Wrong Channel Name or Limit Requests Today"}