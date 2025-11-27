CREATE OR REPLACE DATABASE Spotify_DB;
CREATE OR REPLACE SCHEMA SPOTIFY;

CREATE OR REPLACE TABLE SPOTIFY_DB.PUBLIC.album_data(
    album_id VARCHAR(100),
    album_name VARCHAR(100),
    album_release_data DATE,
    artist_name VARCHAR(100),
    album_url VARCHAR(250)
    );

CREATE OR REPLACE  TABLE SPOTIFY_DB.PUBLIC.artist_data(
    artist_id VARCHAR(100),
    artist_name VARCHAR(100),
    artist_ref VARCHAR(250)
    );

CREATE OR REPLACE TABLE SPOTIFY_DB.PUBLIC.song_data(
    song_id VARCHAR(100),
    song_name VARCHAR(100),
    song_duration VARCHAR(100),
    song_url VARCHAR(250),
    song_popularity INT,
    song_added DATE,
    album_id VARCHAR(100),
    artist_id VARCHAR(100)
    );

    CREATE OR REPLACE SCHEMA EXTERNAL_STORAGE;

    CREATE OR REPLACE STORAGE INTEGRATION  my_storage_int
    TYPE =  EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::083308938413:role/spotify_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://spotify-etl-project-rakesh007/',
    's3://spotify-etl-project-rakesh007/transformed_data/song_data/',
    's3://spotify-etl-project-rakesh007/transformed_data/album_data/',
    's3://spotify-etl-project-rakesh007/transformed_data/artist_data/');

    
    DESC STORAGE INTEGRATION my_storage_int;

    CREATE OR REPLACE FILE FORMAT SPOTIFY_DB.EXTERNAL_STORAGE.CSV_FORMAT
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    FIELD_DELIMITER = ',';

    --Creating state for song_data
        
    CREATE OR REPLACE STAGE SPOTIFY_DB.EXTERNAL_STORAGE.my_stage
    URL = 's3://spotify-etl-project-rakesh007/transformed_data/song_data/'
    STORAGE_INTEGRATION= my_storage_int
    FILE_FORMAT = SPOTIFY_DB.EXTERNAL_STORAGE.CSV_FORMAT;
    
    --testing by manually loading
    COPY INTO SPOTIFY.PUBLIC.song_data
    FROM @SPOTIFY.EXTERNAL_STORAGE.my_stage
    FILE_FORMAT  =SPOTIFY.EXTERNAL_STORAGE.CSV_FORMAT;'''

    -- Creating state for album_data
        
    CREATE OR REPLACE STAGE SPOTIFY_DB.EXTERNAL_STORAGE.my_stage_1
    URL = 's3://spotify-etl-project-rakesh007/transformed_data/album_data/'
    STORAGE_INTEGRATION= my_storage_int
    FILE_FORMAT = SPOTIFY.EXTERNAL_STORAGE.CSV_FORMAT;

        --testing by manually loading
    COPY INTO SPOTIFY.PUBLIC.album_data
    FROM @SPOTIFY.EXTERNAL_STORAGE.my_stage_1
    FILE_FORMAT  =SPOTIFY.EXTERNAL_STORAGE.CSV_FORMAT;

    -- Creating state for artist_data
        
    CREATE OR REPLACE STAGE SPOTIFY_DB.EXTERNAL_STORAGE.my_stage_2
    URL = 's3://spotify-etl-project-rakesh007/transformed_data/artist_data/'
    STORAGE_INTEGRATION= my_storage_int
    FILE_FORMAT = SPOTIFY_DB.EXTERNAL_STORAGE.CSV_FORMAT;

       --testing by manually loading
    COPY INTO SPOTIFY.PUBLIC.artist_data
    FROM @SPOTIFY.EXTERNAL_STORAGE.my_stage_2
    FILE_FORMAT  =SPOTIFY.EXTERNAL_STORAGE.CSV_FORMAT;
    
    LIST @SPOTIFY_DB.EXTERNAL_STORAGE.my_stage;

    LIST @SPOTIFY_DB.EXTERNAL_STORAGE.my_stage_1;

    LIST @SPOTIFY_DB.EXTERNAL_STORAGE.my_stage_2;

    

    -- creating album pipe
    
    CREATE OR REPLACE PIPE spotify_album_pipe
    auto_ingest = TRUE
    AS 
    COPY INTO SPOTIFY_DB.PUBLIC.album_data
    FROM @SPOTIFY_DB.EXTERNAL_STORAGE.my_stage_1
    FILE_FORMAT  =SPOTIFY_DB.EXTERNAL_STORAGE.CSV_FORMAT;

    -- creating song pipe

    CREATE OR REPLACE PIPE spotify_song_pipe
    auto_ingest = TRUE
    AS
    COPY INTO SPOTIFY_DB.PUBLIC.SONG_DATA
    FROM @SPOTIFY_DB.EXTERNAL_STORAGE.my_stage
    FILE_FORMAT = SPOTIFY_DB.EXTERNAL_STORAGE.CSV_FORMAT;
    
    -- creating artist data
     CREATE OR REPLACE PIPE spotify_artist_pipe
     auto_ingest = TRUE
     AS 
     COPY INTO SPOTIFY_DB.PUBLIC.artist_data
     FROM @SPOTIFY_DB.EXTERNAL_STORAGE.my_stage_2
     FILE_FORMAT = SPOTIFY_DB.EXTERNAL_STORAGE.CSV_FORMAT;

     DESC PIPE SPOTIFY_DB.PUBLIC.spotify_album_pipe;

     DESC PIPE SPOTIFY_DB.PUBLIC.SPOTIFY_ARTIST_PIPE;

     DESC PIPE SPOTIFY_DB.PUBLIC.SPOTIFY_SONG_PIPE;


     SHOW PIPES IN SCHEMA SPOTIFY_DB.PUBLIC;
     
     
    select * from album_data;

