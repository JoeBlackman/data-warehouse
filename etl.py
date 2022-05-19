"""
This module is responsible for extracting data from the s3 bucket and transforming and loading it into 
staging tables, then analysis tables
"""

import copy
from datetime import datetime, date
from unicodedata import decimal
import boto3
from debugpy import connect
from decimal import Decimal
import logging
from numpy import int64, float64, NaN
import pandas as pd
import pathlib
import psycopg2
from psycopg2.errors import NotNullViolation
from pyparsing import col
import sqlalchemy as sa
from sqlalchemy.engine.url import URL
#from sqlalchemy import create_engine
import sql_queries
from sql_queries import copy_table_queries, insert_table_queries
from string import Template
import sys

import config


logging.basicConfig(
    filename=pathlib.Path('logs', f'{date.today()}.log'),
    encoding='utf-8',
    level=logging.DEBUG,
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p'
)


def load_staging_tables(conn):
    """
    This method will load data into the staging tables from the s3 bucket
    """
    logging.info(
        'Copying data from s3 to staging_events and staging_songs tables')
    for query in copy_table_queries:
        try:
            with conn.cursor() as cur:
                cur.execute(query)
        except Exception as e:
            logging.error(
                f'Exception {e} occurred on execution of query: {query}')
            sys.exit(1)


def insert_tables(conn):
    """
    This method collects all items from staging_songs and staging events and decides 
    what should be stored in analytical tables and how it should be stored
    """
    # get rows from staging_songs table then load the result into a pandas dataframe
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM staging_songs;')
        staging_songs_data = cur.fetchall()
    staging_songs_df = pd.DataFrame(
        staging_songs_data,
        columns=[
            'num_songs', 'artist_id', 'artist_latitude', 'artist_longitude',
            'artist_location', 'artist_name', 'song_id', 'title', 'duration', 'year'
        ]
    )

    # load data from staging_songs table into analysis tables for songs and artists
    logging.info('Loading songs and artists tables')
    for index, row in staging_songs_df.iterrows():
        data = formatForRedshift(row)
        load_song(conn, data)
        load_artist(conn, data)
    logging.info('Completed loading songs and artists tables')

    # get rows from staging_events table then load the result into a pandas dataframe
    with conn.cursor() as cur:
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


    staging_events_df['registration'] = staging_events_df['registration'].fillna(0).astype(int64)
    staging_events_df['userId'] = staging_events_df['userId'].fillna(0).astype(int64)
    staging_events_df['registration'].replace(0, NaN)
    staging_events_df['userId'].replace(0, NaN)

    # load data from staging_events table into analysis tables for users, time, and songplays
    logging.info('Loading users, time, and songplays tables')
    for index, row in staging_events_df.iterrows():
        # only load data if row['page'] == 'NextSong'. other events don't matter.
        if row['page'] == 'NextSong':
            data = formatForRedshift(row)
            load_user(conn, data)
            load_time(conn, data)
            load_songplay(conn, data)
    logging.info('Completed loading users, time, and songplays tables!')

    # after tables have all been loaded, delete duplicate entries
    cleanup_duplicates(conn)


def load_song(conn, data):
    """
    Build query string for insertion into song table
    Insert song data into song table
    """
    insert_songs_t = Template(sql_queries.song_table_insert)
    insert_songs = insert_songs_t.substitute(
        song_id=data['song_id'],
        title=data['title'],
        artist_id=data['artist_id'],
        year=data['year'],
        duration=data['duration']
    )
    try:
        with conn.cursor() as cur:
            cur.execute(insert_songs)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_songs}')
        sys.exit(1)


def load_artist(conn, data):
    """
    Build query string for insertion into artist table
    Insert artist data into artist table
    """
    insert_artists_t = Template(sql_queries.artist_table_insert)
    insert_artists = insert_artists_t.substitute(
        artist_id=data['artist_id'],
        name=data['artist_name'],
        location=data['artist_location'],
        latitude=data['artist_latitude'],
        longitude=data['artist_longitude']
    )
    try:
        with conn.cursor() as cur:
            cur.execute(insert_artists)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_artists}')
        sys.exit(1)


def load_user(conn, data):
    """
    Build query string for insertion into user table
    Insert user data into user table
    """
    insert_users_t = Template(sql_queries.user_table_insert)
    insert_users = insert_users_t.substitute(
        user_id=data['userId'],
        first_name=data['firstName'],
        last_name=data['lastName'],
        gender=data['gender'],
        level=data['level']
    )
    try:
        with conn.cursor() as cur:
            cur.execute(insert_users)
    except NotNullViolation as nn:
        logging.error(
            f"""NotNullViolation {nn} occurred on execution of query: {insert_users}
            Likely an instance of event without a user session. Skipping insertion.""")
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_users}')
        sys.exit(1)


def load_time(conn, data):
    """
    Transform timestamp from dataframe into new fields
    Build query string for insertion into time table
    Insert time data into time table
    """
    # transform ts into new fields for time table
    try:
        t = pd.to_datetime(data['ts'], unit='ms')
    except Exception as e:
        logging.error(f'Error on converting timestamp to datetime: {e}')
        sys.exit(1)
    insert_time_t = Template(sql_queries.time_table_insert)
    insert_time = insert_time_t.substitute(
        start_time=f'\'{t}\'',
        hour=t.hour,
        day=t.day,
        week=t.week,
        month=t.month,
        year=t.year,
        weekday=t.day_of_week
    )
    try:
        with conn.cursor() as cur:
            cur.execute(insert_time)
    except psycopg2.errors.SyntaxError as e:
        logging.error(f'Syntax Error on execution: {insert_time}')
        logging.error(f'Trace: {e}')
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_time}')
        sys.exit(1)


def load_songplay(conn, data):
    # get the time_id of the last inserted time
    with conn.cursor() as cur:
        cur.execute(sql_queries.get_last_time_id)
        t = cur.fetchone()
        time_id = t[0] if t is not None else 'Null'
        # get the song_id from songs table with matching song and artist name
        cur.execute(Template(sql_queries.get_artist_id).substitute(
            name=data['artist']))
        a = cur.fetchone()
        artist_id = a[0] if a is not None else 'Null'
        # get the artist_id from the artist table with matching artist name and location
        cur.execute(Template(sql_queries.get_song_id).substitute(
            title=data['song'], artist_id=f'$artist_id${artist_id}$artist_id$'))
        s = cur.fetchone()
        song_id = s[0] if s is not None else 'Null'
    if (time_id == 'Null') or (artist_id == 'Null') or (song_id == 'Null'):
        # need to be able to relate to have a meaningful event, don't try to insert
        # if info is incomplete
        return
    insert_songplays_t = Template(sql_queries.songplay_table_insert)
    insert_songplays = insert_songplays_t.substitute(
        time_id=time_id,
        user_id=data['userId'],
        level=data['level'],
        song_id=song_id,
        artist_id=artist_id,
        session_id=data['sessionId'],
        location=data['location'],
        user_agent=data['userAgent'],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(insert_songplays)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {insert_songplays}')
        sys.exit(1)


def cleanup_duplicates(conn):
    logging.info(
        'Cleaning up duplicates in users, artists, and songs tables...')
    try:
        with conn.cursor() as cur:
            cur.execute(sql_queries.clean_up_duplicate_users)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {sql_queries.clean_up_duplicate_users}')
    try:
        with conn.cursor() as cur:
            cur.execute(sql_queries.clean_up_duplicate_artists)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {sql_queries.clean_up_duplicate_artists}')
    try:
        with conn.cursor() as cur:
            cur.execute(sql_queries.clean_up_duplicate_songs)
    except Exception as e:
        logging.error(
            f'Exception {e} occurred on execution of query: {sql_queries.clean_up_duplicate_songs}')
    logging.info(
        'Completed cleaning up duplicates in users, time, and songs tables!')


def formatForRedshift(row):
    """
    replace None with 'null' 
    escape all strings with dollar quoted tags
    """
    # shallow copy row to data so the data origin isn't modified
    data = copy.copy(row)
    for field in data.index:
        if (data[field] is None) or (data[field] in ['NaN', 'Nan', 'nan']):
            data[field] = 'null'
        elif type(data[field]) in [int, int64, float, float64, Decimal]:
            # non null bool, int, and float don't need escapes
            continue
        else:
            data[field] = f'${field}${data[field]}${field}$'
    return data


def main():
    """
    The entry point to the code when this modules is called from the command line.
    """
    etl_start_time = datetime.now()
    logging.info(f'Starting ETL run at {etl_start_time}')

    # configure connection
    conn = psycopg2.connect(f"""
        host={config.ENDPOINT}
        dbname={config.DB_NAME}
        user={config.USER}
        password={config.PW}
        port={config.PORT}
    """)
    conn.autocommit = True

    # EXTRACT
    # Staging data extracted from s3 bucket to staging database in redshift
    load_staging_tables(conn)

    # TRANSFORM/LOAD
    # Data from staging database loaded into production database (star schema)
    insert_tables(conn)

    conn.close()
    logging.info(f'End of ETL that began at {etl_start_time}')


if __name__ == "__main__":
    main()
