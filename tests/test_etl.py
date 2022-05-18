#from xmlrpc.client import DateTime
import datetime
from numpy import character, int64, float64
import pandas as pd
import pathlib
import psycopg2
from psycopg2 import Timestamp, extensions
import pytest
import sys

project_dir = pathlib.Path(__file__).parent.resolve().parent.resolve()
sys.path.insert(0, str(project_dir))
print(sys.path)

import etl
import sql_test_setup


@pytest.fixture(scope='module')
def connection_handler():
    # ensure scratchdb exists first, then return a connection to a database session with it
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=student password=student")
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP DATABASE IF EXISTS dwhtestdb;")
        cur.execute("CREATE DATABASE dwhtestdb;")
    conn.close()

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=dwhtestdb user=student password=student")
    #cur = conn.cursor()

    # create tables will be different for postgres vs redshift
    #cur.execute(sql_queries.staging_songs_table_create)
    #cur.execute(sql_queries.staging_events_table_create)
    with conn.cursor() as cur:
        cur.execute(sql_test_setup.song_table_create)
        cur.execute(sql_test_setup.artist_table_create)
        cur.execute(sql_test_setup.user_table_create)
        cur.execute(sql_test_setup.time_table_create)
        cur.execute(sql_test_setup.songplay_table_create)

    yield conn

    # tear down
    #with conn.cursor() as cur:
    #    cur.execute("DROP DATABASE dwhtestdb;")
    conn.close()


@pytest.mark.order(1)
def test_schema(connection_handler):
    """
    tests for schema setup for correctness
    """
    with connection_handler.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
        """
        )
        tables = cur.fetchall()
        for table in tables:
            assert table[0] in ['songs', 'artists', 'users', 'time', 'songplays']


@pytest.mark.order(2)
def test_load_song(connection_handler):
    """
    tests functionality of etl.load_song
    """
    df = pd.read_csv('tests/staging_songs.csv')
    series = df.iloc[0]
    data = etl.formatForRedshift(series)
    etl.load_song(connection_handler, data)

    with connection_handler as conn:
        songs = pd.read_sql(con=conn, sql='SELECT * FROM songs;')
    song = songs.iloc[0]
    assert len(songs) == 1
    assert type(song['song_id']) == str
    assert type(song['title']) == str
    assert type(song['artist_id']) == str
    assert type(song['year']) == int64
    assert type(song['duration']) == float64


@pytest.mark.order(3)
def test_load_artist(connection_handler):
    """
    tests functionality of etl.load_artist
    """
    df = pd.read_csv('tests/staging_songs.csv')
    series = df.iloc[0]
    data = etl.formatForRedshift(series)
    etl.load_artist(connection_handler, data)

    with connection_handler as conn:
        artists = pd.read_sql(con=conn, sql='SELECT * FROM artists;')
    artist = artists.iloc[0]
    assert len(artists) == 1
    assert type(artist['artist_id']) == str
    assert type(artist['name']) == str
    assert type(artist['location']) == str
    assert type(artist['latitude']) == float64
    assert type(artist['longitude']) == float64


@pytest.mark.order(4)
def test_load_user(connection_handler):
    """
    tests functionality of etl.load_user
    """
    df = pd.read_csv('tests/staging_events.csv')
    series = df.iloc[0]
    data = etl.formatForRedshift(series)
    etl.load_user(connection_handler, data)

    with connection_handler as conn:
        users = pd.read_sql(con=conn, sql='SELECT * FROM users;')
    user = users.iloc[0]
    assert len(users) == 1
    assert type(user['user_id']) == int64
    assert type(user['first_name']) == str
    assert type(user['last_name']) == str
    assert type(user['gender']) == str
    assert type(user['level']) == str


@pytest.mark.order(5)
def test_load_time(connection_handler):
    """
    tests functionality of load_time
    """

    df = pd.read_csv('tests/staging_events.csv')
    data = df.iloc[0]
    etl.load_time(connection_handler, data)

    with connection_handler as conn:
        times = pd.read_sql(con=conn, sql='SELECT * FROM time;')
    time = times.iloc[0]
    assert len(times) == 1
    assert type(time['time_id']) == int64
    assert type(time['start_time']) == pd._libs.tslibs.timestamps.Timestamp
    assert type(time['hour']) == int64
    assert type(time['day']) == int64
    assert type(time['week']) == int64
    assert type(time['month']) == int64
    assert type(time['year']) == int64
    assert type(time['weekday']) == int64


#def test_load_songplay(connection_handler):
#    """
#    tests functionality of load_songplays
#    """
#    
#    df = pd.read_csv('tests/staging_events.csv')
#    series = df.iloc[0]
#    data = etl.formatForRedshift(series)
#    etl.load_songplay(connection_handler, data)
#
#    with connection_handler as conn:
#        songplays = pd.read_sql(con=conn, sql='SELECT * FROM songplays;')
#    songplay = songplays.iloc[0]
#    assert len(songplays) == 1


# def test_insert_tables(conn):
#    cur = conn.cursor()
#    etl.insert_tables()
