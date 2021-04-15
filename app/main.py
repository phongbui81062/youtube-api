import os
import logging
import uvicorn
from fastapi import FastAPI, BackgroundTasks
from celery import group
from .worker.celery_app import celery_app
import requests
import pandas as pd
from .worker.apply_text_analytic import get_data

log = logging.getLogger(__name__)

app = FastAPI()



# @app.get("/apply_analytic")
# async def analytic():
#     async with aiohttp.ClientSession() as session:
#         response = await asyncio.gather(*[get_data(str(i),text,session) for i,text in enumerate(data['Comment'])])
#     df = pd.DataFrame(list(chain(*list(map(lambda x: x['documents'],response)))))
#     df = df['confidenceScores'].apply(pd.Series)
#     print(df)
#     return df


@app.get("/videoId")
def test(url: str, channel_name:str,developer_key: str = "AIzaSyDCqHr2yTa_sgpnlQgpfYMio2tKXgFlZr4"):
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
        task_name = "get_list_videos"
        videos = zip(video_ids, video_titles)
        group(celery_app.send_task(task_name, args=[
                    vid_id, vid_title,channel_name,'']) for vid_id, vid_title in videos)()
        return {"Error": "Wrong Channel Name or Limit Requests Today"}
    else:
        return {"Error": "Wrong Channel Name or Limit Requests Today"}
