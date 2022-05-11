"""
This module is responsible for extracting data from the s3 bucket and transforming and loading it into 
staging tables, then analysis tables
"""

from datetime import datetime, date
import boto3
from debugpy import connect
import logging
import pandas as pd
import pathlib
import psycopg2
from pyparsing import col
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
#from sqlalchemy import create_engine
import sql_queries
from sql_queries import copy_table_queries, insert_table_queries
from string import Template

import config


logging.basicConfig(
    filename=pathlib.Path('logs', f'{date.today()}.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)

# source:
# https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/


def load_staging_tables(cur, conn):
    """
    This method will load data into the staging tables from the s3 bucket
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This method will load data into the analysis tables from the staging tables.
    """

    # engine = sa.create_engine(
    #    f'postgresql://{config.USER}:{config.PW}@{config.ENDPOINT}:{config.PORT}/{config.DB_NAME}')

    cur.execute('SELECT * FROM staging_songs;')
    staging_songs_data = cur.fetchall()
    staging_songs_df = pd.DataFrame(
        staging_songs_data,
        columns=[
            'num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
            'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year'
        ]
    )
    # get rows from staging_songs table
    # only using sql alchemy because of this pandas.read_sql_query method
    # would be better to use one pattern to access database, not two
    # could be beneficial to use cur and conn for the selection of staging_songs,
    # then load the result into a pandas dataframe
    # staging_songs_df = pd.read_sql_query(
    #    'SELECT * FROM staging_songs;', con=engine)
    # set headers of staging_songs_df
    # staging_songs_df.columns = [
    #    'num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
    #    'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year'
    # ]
    # load data from staging_songs table into analysis tables for songs and artists
    logging.info('Loading songs and artists tables from staging_songs table')
    for index, row in staging_songs_df.iterrows():
        #insert_songs_t = Template(sql_queries.song_table_insert)
        # insert_songs = insert_songs_t.substitute(
        #    song_id=row['song_id'],
        #    title=row['title'],
        #    artist_id=row['artist_id'],
        #    year=row['year'],
        #    duration=row['duration']
        # )
        # try:
        #    cur.execute(insert_songs)
        # except Exception as e:
        #    print(f'Error on query: {insert_songs}')
        #    print(e)
        load_song(cur, row)
        # with engine.connect() as connection:
        #    try:
        #        connection.execute(sa.text(insert_songs))
        #    except Exception as e:
        #        print(f'Error on query: {insert_songs}')
        #        print(e)
        #insert_artists_t = Template(sql_queries.artist_table_insert)
        # insert_artists = insert_artists_t.substitute(
        #    artist_id=row['artist_id'],
        #    name=row['artist_name'],
        #    location=row['artist_location'],
        #    latitude=row['artist_latitude'],
        #    longitude=row['artist_longitude']
        # )
        # try:
        #    cur.execute(insert_artists)
        # except Exception as e:
        #    print(f'Error on query: {insert_artists}')
        #    print(e)
        load_artist(cur, row)
        # with engine.connect() as connection:
        #    try:
        #        connection.execute(sa.text(insert_artists))
        #    except Exception as e:
        #        print(f'Error on query: {insert_artists}')
        #        print(e)
    logging.info('Completed loading songs and artists tables')

    cur.execute('SELECT * FROM staging_events;')
    staging_events_data = cur.fetchall()
    staging_events_df = pd.DataFrame(
        staging_events_data,
        columns=[
            'artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
            'length', 'level', 'location', 'method', 'page', 'registration',
            'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'
        ]
    )
    # staging_events_df = pd.read_sql_query(
    #    'SELECT * FROM staging_events;', con=engine)
    # set headers of staging_events_df
    # staging_events_df.columns = [
    #    'artist', 'auth', 'firstName', 'gender', 'itemInSession', 'lastName',
    #    'length', 'level', 'location', 'method', 'page', 'registration',
    #    'sessionId', 'song', 'status', 'ts', 'userAgent', 'userId'
    # ]
    # load data from staging_events table into analysis tables for users, time, and songplays
    logging.info(
        'Loading users, time, and songplays tables from staging_events table')
    for index, row in staging_events_df.iterrows():
        #insert_users_t = Template(sql_queries.user_table_insert)
        # insert_users = insert_users_t.substitute(
        #    user_id=int(row['userId']),
        #    first_name=row['firstName'],
        #    last_name=row['lastName'],
        #    gender=row['gender'],
        #    level=row['level']
        # )
        # try:
        #    cur.execute(insert_users)
        # except Exception as e:
        #    print(f'Error on query: {insert_users}')
        #    print(e)
        load_user(cur, row)
        # with engine.connect() as connection:
        #    try:
        #        connection.execute(sa.text(insert_users))
        #    except Exception as e:
        #        print(f'Error on query: {insert_users}')
        #        print(e)
        #insert_time_t = Template(sql_queries.time_table_insert)
        # transform ts into new fields for time table
        #t = pd.to_datetime(row['ts'], unit='ms')
        # insert_time = insert_time_t.substitute(
        #    start_time=t,
        #    hour=t.hour,
        #    day=t.day,
        #    week=t.week,
        #    month=t.month,
        #    year=t.year,
        #    weekday=t.day_of_week
        # )
        # cur.execute(insert_time)
        load_time(cur, row)
        # with engine.connect() as connection:
        #    connection.execute(sa.text(insert_time))
        #insert_songplays_t = Template(sql_queries.songplay_table_insert)
        # get the time_id of the last inserted time
        #time_id = cur.execute(sql_queries.get_last_time_id).fetchone()[0]
        # with engine.connect() as connection:
        #    result = connection.execute(sa.text(sql_queries.get_last_time_id))
        #    r = result.fetchone()
        #    time_id = row[0]
        #time_id = row._mapping[time.c.time_id]
        # get the song_id from songs table with matching song and artist name
        # artist_id = cur.execute(Template(sql_queries.get_artist_id).substitute(
        #    name=row['artist'])).fetchone()[0]
        # with engine.connect() as connection:
        #    result = connection.execute(
        #        sa.text(Template(sql_queries.get_artist_id).substitute(name=row['artist'])))
        #    r = result.fetchone()
        #    artist_id = row[0]
        # get the artist_id from the artist table with matching artist name and location
        # song_id = cur.execute(Template(sql_queries.get_song_id).substitute(
        #    title=row['song'], artist_id=artist_id)).fetchone()[0]
        # with engine.connect() as connection:
        #    # should title be song?
        #    result = connection.execute(
        #        sa.text(Template(sql_queries.get_song_id).substitute(title=row['song'], artist_id=artist_id)))
        #    r = result.fetchone()
        #    song_id = row[0]
        # insert_songplays = insert_songplays_t.substitute(
        #    time_id=time_id,
        #    user_id=row['userId'],
        #    level=row['level'],
        #    song_id=song_id,
        #    artist_id=artist_id,
        #    session_id=row['sessionId'],
        #    location=row['location'],
        #    user_agent=row['userAgent'],
        # )
        # try:
        #    cur.execute(insert_songplays)
        # except Exception as e:
        #    print(f'Error on query: {insert_songplays}')
        #    print(e)
        # with engine.connect() as connection:
        #    try:
        #        connection.execute(sa.text(insert_songplays))
        #    except Exception as e:
        #        print(f'Error on query: {insert_songplays}')
        #        print(e)
        load_songplay(cur, row)
    logging.info('Completed loading users, time, and songplays tables!')
    logging.info(
        'Cleaning up duplicates in users, artists, and songs tables...')
    cur.execute(sql_queries.clean_up_duplicate_users)
    cur.execute(sql_queries.clean_up_duplicate_artists)
    cur.execute(sql_queries.clean_up_duplicate_songs)
    # with engine.connect() as connection:
    #    connection.execute(sa.text(sql_queries.clean_up_duplicate_users))
    #    connection.execute(sa.text(sql_queries.clean_up_duplicate_artists))
    #    connection.execute(sa.text(sql_queries.clean_up_duplicate_songs))
    logging.info(
        'Completed cleaning up duplicates in users, time, and songs tables!')


def load_song(cur, row):
    insert_songs_t = Template(sql_queries.song_table_insert)
    insert_songs = insert_songs_t.substitute(
        song_id=row['song_id'],
        title=row['title'],
        artist_id=row['artist_id'],
        year=row['year'],
        duration=row['duration']
    )
    # with engine.connect() as connection:
    #    try:
    #        connection.execute(sa.text(insert_songs))
    #    except Exception as e:
    #        # might be preferable to use a logger instead of printing to console
    #        print(f'Error on query: {insert_songs}')
    #        print(e)
    try:
        cur.execute(insert_songs)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_songs}')


def load_artist(cur, row):
    insert_artists_t = Template(sql_queries.artist_table_insert)
    insert_artists = insert_artists_t.substitute(
        artist_id=row['artist_id'],
        name=row['artist_name'],
        location=row['artist_location'],
        latitude=row['artist_latitude'],
        longitude=row['artist_longitude']
    )
    try:
        cur.execute(insert_artists)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_artists}')
    # with engine.connect() as connection:
    #    try:
    #        connection.execute(sa.text(insert_artists))
    #    except Exception as e:
    #        # might be preferable to use a logger instead of printing to console
    #        print(f'Error on query: {insert_artists}')
    #        print(e)


def load_user(cur, row):
    insert_users_t = Template(sql_queries.user_table_insert)
    insert_users = insert_users_t.substitute(
        user_id=int(row['userId']),
        first_name=row['firstName'],
        last_name=row['lastName'],
        gender=row['gender'],
        level=row['level']
    )
    try:
        cur.execute(insert_users)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_users}')
    # with engine.connect() as connection:
    #    try:
    #        connection.execute(sa.text(insert_users))
    #    except Exception as e:
    #        # might be preferable to use a logger instead of printing to console
    #        print(f'Error on query: {insert_users}')
    #        print(e)


def load_time(cur, row):
    insert_time_t = Template(sql_queries.time_table_insert)
    # transform ts into new fields for time table
    t = pd.to_datetime(row['ts'], unit='ms')
    insert_time = insert_time_t.substitute(
        start_time=t,
        hour=t.hour,
        day=t.day,
        week=t.week,
        month=t.month,
        year=t.year,
        weekday=t.day_of_week
    )
    cur.execute(insert_time)


def load_songplay(cur, row):
    #insert_songplays_t = Template(sql_queries.songplay_table_insert)
    # get the time_id of the last inserted time
    # with engine.connect() as connection:
    #    result = connection.execute(sa.text(sql_queries.get_last_time_id))
    #    r = result.fetchone()
    #    time_id = row[0]
    #    #time_id = row._mapping[time.c.time_id]
    # get the song_id from songs table with matching song and artist name
    # with engine.connect() as connection:
    #    result = connection.execute(
    #        sa.text(Template(sql_queries.get_artist_id).substitute(name=row['artist'])))
    #    r = result.fetchone()
    #    artist_id = row[0]
    #    # get the artist_id from the artist table with matching artist name and location
    # with engine.connect() as connection:
    # should title be song?
    #    result = connection.execute(
    #        sa.text(Template(sql_queries.get_song_id).substitute(title=row['song'], artist_id=artist_id)))
    #    r = result.fetchone()
    #    song_id = row[0]
    # insert_songplays = insert_songplays_t.substitute(
    #    time_id=time_id,
    #    user_id=row['userId'],
    #    level=row['level'],
    #    song_id=song_id,
    #    artist_id=artist_id,
    #    session_id=row['sessionId'],
    #    location=row['location'],
    #    user_agent=row['userAgent'],
    # )
    # with engine.connect() as connection:
    #    try:
    #        connection.execute(sa.text(insert_songplays))
    #    except Exception as e:
    #        # might be preferable to use a logger instead of printing to console
    #        print(f'Error on query: {insert_songplays}')
    #        print(e)
    insert_songplays_t = Template(sql_queries.songplay_table_insert)
    # get the time_id of the last inserted time
    time_id = cur.execute(sql_queries.get_last_time_id).fetchone()[0]
    # get the song_id from songs table with matching song and artist name
    artist_id = cur.execute(Template(sql_queries.get_artist_id).substitute(
        name=row['artist'])).fetchone()[0]
    # get the artist_id from the artist table with matching artist name and location
    song_id = cur.execute(Template(sql_queries.get_song_id).substitute(
        title=row['song'], artist_id=artist_id)).fetchone()[0]
    insert_songplays = insert_songplays_t.substitute(
        time_id=time_id,
        user_id=row['userId'],
        level=row['level'],
        song_id=song_id,
        artist_id=artist_id,
        session_id=row['sessionId'],
        location=row['location'],
        user_agent=row['userAgent'],
    )
    try:
        cur.execute(insert_songplays)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_songplays}')


def main():
    """
    The entry point to the code when this modules is called from the command line.
    """
    etl_start_time = datetime.now()
    logging.info(f'Starting ETL run at {etl_start_time}')

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
    insert_tables(cur, conn)

    conn.close()
    logging.info(f'End of ETL that began at {etl_start_time}')


if __name__ == "__main__":
    main()
