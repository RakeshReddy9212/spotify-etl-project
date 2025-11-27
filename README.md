# spotify-etl-project
Spotify ETL that extracts playlist data via Spotipy in an AWS Lambda, stores raw JSON to S3, transforms records with a second Lambda into CSVs, and auto-loads transformed files into Snowflake using Snowflake stages/pipes.
