import os
from googleapiclient.discovery import build
import pandas as pd 
import json
from datetime import datetime
import s3fs 



def run_youtube_etl():

    api_key="your_youtube_api_key"


    youtube=build('youtube','v3',developerKey=api_key)

    channel_id = "UC_x5XG1OV2P6uZZ5FSM9Ttw"

    request=youtube.search().list(
        part="snippet",
        channelId=channel_id,
        maxResults=25,
        order="date"
    )

    response=request.execute() 

    video_titles=[]

    for i in response['items']:
        if i['id']['kind']=='youtube#video':
            video_data={
                'video_id':i['id']['videoId'],
                'title':i['snippet']['title'],
                'published_at':i['snippet']['publishedAt'],
                'description':i['snippet']['description']
            }
            video_titles.append(video_data)



        
    df=pd.DataFrame(video_titles)
    df.to_csv("s3://your_bucket_name/youtube_project.csv",index=False)

   






    
