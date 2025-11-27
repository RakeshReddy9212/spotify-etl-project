import json
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime

def album(data):
    album_list =[]
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_data = row['track']['album']['release_date']
        artist_name = row['track']['album']['artists'][0]['name']
        album_url = row['track']['album']['external_urls']['spotify']
        album_element = {'album_id' : album_id, 'album_name' : album_name, 'album_release_data':
                        album_release_data, 'artist_name' : artist_name, 'album_url' : album_url}
        album_list.append(album_element)

    return album_list

def artist(data):
    artist_list = []
    for row in data['items']:
            for key, value in row.items():
                if key == 'track':
                    for artist in value['artists']:
                        artist_dist = { 'artist_id' : artist['id'],'artist_name' : artist['name'], 'artist_ref' : artist['href']  } 
                        artist_list.append(artist_dist)
    return artist_list

def song(data):
    song_list = []
    for row in data['items']:
            song_id = row['track']['id']
            song_name = row['track']['name']
            song_duration = row['track']['duration_ms']
            song_url = row['track']['external_urls']['spotify']
            song_popularity = row['track']['popularity']
            song_added = row['added_at']
            album_id = row['track']['album']['id']
            artist_id = row['track']['album']['artists'][0]['id']
            song_elements = {'song_id':song_id, 'song_name':song_name,
                            'song_duration':song_duration, 'song_url':song_url, 'song_popularity': song_popularity, 'song_added':song_added, 'album_id':album_id, 'artist_id':artist_id}
            song_list.append(song_elements)
    return song_list

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket = 'spotify-etl-project-rakesh007'
    key = 'raw_data/to_be_processed/'

    spotify_data = []
    spotify_keys = []
    for file in s3.list_objects(Bucket='spotify-etl-project-rakesh007', Prefix='raw_data/to_be_processed/')['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object(Bucket=bucket, Key=file_key)
            content = response['Body']
            jsonObject= json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    for data in spotify_data:
        album_list = album(data)
        song_list = song(data)
        artist_list = artist(data)
        
        album_df = pd.DataFrame.from_dict(album_list)
        artist_df = pd.DataFrame(artist_list)
        song_df = pd.DataFrame(song_list)

        artist_df.drop_duplicates(inplace=True)

        #album_df.drop_duplicates(inplace = True)
        
        album_df['album_release_data'] = pd.to_datetime(
        album_df['album_release_data'], errors='coerce')

        # Drop rows where conversion failed (NaT) or values were missing
        album_df = album_df.dropna(subset=['album_release_data'])


        song_df['song_added'] = pd.to_datetime(song_df['song_added'])

        song_key ="transformed_data/song_data/song_transformed"+ str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))+".csv"
        album_key = "transformed_data/album_data/album_transformed"+ str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))+".csv"
        artist_key = "transformed_data/artist_data/artist_transformed"+ str(datetime.now().strftime("%Y-%m-%d_%H-%M-%S"))+".csv"

        song_buffer = StringIO()
        song_df.to_csv(song_buffer, index=False)
        song_content = song_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=song_key, Body=song_content)

        album_buffer = StringIO()
        album_df.to_csv(album_buffer,index=False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=bucket, Key=album_key,Body=album_content)

        artist_buffer =StringIO()
        artist_df.to_csv(artist_buffer, index=False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket = bucket, Key = artist_key, Body = artist_content)
    
    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {'Bucket':bucket, 'Key':key}
        s3_resource.meta.client.copy(copy_source,bucket,'raw_data/processed_data/' + key.split("/")[-1])
        s3_resource.Object(bucket,key).delete()
