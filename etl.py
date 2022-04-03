"""
This module is responsible for extracting data from the s3 bucket and transforming and loading it into 
staging tables, then analysis tables
"""

import boto3
from debugpy import connect
import pandas as pd
import psycopg2
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
#from sqlalchemy import create_engine
import sql_queries
from sql_queries import copy_table_queries, insert_table_queries
from string import Template

import config

# source:
# https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/


def load_staging_tables(cur, conn):
    """
    This method will load data into the staging tables from the s3 bucket
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables():
    """
    This method will load data into the analysis tables from the staging tables.
    """
    # url = URL.create(
    #    drivername='redshift+redshift_connector',
    #    host='<>..<aws-region>.redshift.amazonaws.com',
    #    port=5439,
    #    database='',
    #    username='',
    #    password=''
    # )
    engine = sa.create_engine(
        f'postgresql://{config.USER}:{config.PW}@{config.DWH_ENDPOINT}:{config.PORT}/{config.DB_NAME}')

    # get rows from staging_songs table
    staging_songs_df = pd.read_sql_query(
        'SELECT * FROM staging_songs;', con=engine)
    # set headers of staging_songs_df
    staging_songs_df.columns = [
        'num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
        'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year'
    ]
    # load data from staging_songs table into analysis tables for songs and artists
    for row in staging_songs_df:
        insert_songs = Template(sql_queries.song_table_insert)
        insert_songs.substitute(
            song_id=row['song_id'],
            title=row['title'],
            artist_id=row['artist_id'],
            year=row['year'],
            duration=row['duration']
        )
        with engine.connect() as connection:
            connection.execute(insert_songs)
        insert_artists = Template(sql_queries.artist_table_insert)
        insert_artists.substitute(
            artist_id=row['artist_id'],
            name=row['artist_name'],
            location=row['artist_location'],
            latitude=row['artist_latitude'],
            longitude=row['artist_longitude']
        )
        with engine.connect() as connection:
            connection.execute(insert_artists)
    staging_events_df = pd.read_sql_query(
        'SELECT * FROM staging_events;', con=engine)
    # set headers of staging_events_df
    staging_events_df.columns = [
        'artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
        'length', 'level', 'location', 'method', 'page', 'registration',
        'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'
    ]
    # load data from staging_events table into analysis tables for users, time, and songplays
    for row in staging_events_df:
        insert_users = Template(sql_queries.user_table_insert)
        insert_users.substitute(
            user_id=row['userId'],
            first_name=row['firstName'],
            last_name=row['lastName'],
            gender=row['gender'],
            level=row['level']
        )
        with engine.connect() as connection:
            connection.execute(insert_users)
        insert_time = Template(sql_queries.time_table_insert)
        # transform ts into new fields for time table
        t = pd.to_datetime(row['ts'], unit='ms')
        insert_time.substitute(
            start_time=t,
            hour=t.dt.hour,
            day=t.dt.day,
            week=t.dt.week,
            month=t.dt.month,
            year=t.dt.year,
            weekday=t.dt.weekday
        )
        with engine.connect() as connection:
            connection.execute(insert_time)
        insert_songplays = Template(sql_queries.songplay_table_insert)
        # get the time_id of the last inserted time
        with engine.connect() as connection:
            result = connection.execute(sql_queries.get_last_time_id)
            r = result.fetchone()
            time_id = row[0]
            #time_id = row._mapping[time.c.time_id]
        # get the song_id from songs table with matching song and artist name
        with engine.connect() as connection:
            result = connection.execute(
                Template(sql_queries.get_artist_id).substitute(name=row['artist']))
            r = result.fetchone()
            artist_id = row[0]
        # get the artist_id from the artist table with matching artist name and location
        with engine.connect() as connection:
            result = connection.execute(
                Template(sql_queries.get_song_id).substitute(title=row['title'], artist_id=artist_id))
            r = result.fetchone()
            song_id = row[0]
        insert_songplays.substitute(
            time_id=time_id,
            user_id=row['userId'],
            level=row['level'],
            song_id=song_id,
            artist_id=artist_id,
            session_id=row['sessionId'],
            location=row['location'],
            user_agent=row['sessionId'],
        )
    # for query in insert_table_queries:
    #    cur.execute(query)
    #    conn.commit()


def main():
    """
    The entry point to the code when this modules is called from the command line.
    """
    conn = psycopg2.connect(f"""
        host={config.ENDPOINT} 
        dbname={config.DB_NAME} 
        user={config.USER} 
        password={config.PW} 
        port={config.PORT}
    """)
    cur = conn.cursor()

    # EXTRACT
    # Staging data extracted from s3 bucket to staging database in redshift
    load_staging_tables(cur, conn)

    # TRANSFORM/LOAD
    # Data from staging database loaded into production database (star schema)
    insert_tables()

    conn.close()


if __name__ == "__main__":
    main()
