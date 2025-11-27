import json
import spotipy
import os
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime

def lambda_handler(event, context):
    client_id = os.environ['client_id']
    client_secret = os.environ['client_secret']
    
    credentials_manager = SpotifyClientCredentials(client_id = client_id,client_secret= client_secret   )
    sp = spotipy.Spotify(client_credentials_manager=credentials_manager)
    
    playlist_link = 'https://open.spotify.com/playlist/5ABHKGoOzxkaa28ttQV9sE'
    URI = playlist_link.split("/")[-1]

    data = sp.playlist_tracks(URI)
    
    client = boto3.client('s3')
    file_name = 'spotify_raw' + str(datetime.now()) + '.json'
    client.put_object(
        Bucket='spotify-etl-project-rakesh007',
        Key='raw_data/to_be_processed/' + file_name,
        Body=json.dumps(data)
    )