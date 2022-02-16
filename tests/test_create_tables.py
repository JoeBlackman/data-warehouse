"""
This module is intended to test the create_tables.py module.
"""

import configparser
import psycopg2
import pytest

import create_tables


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
cluster = config['CLUSTER']


#@pytest.fixture
#def connection_handler():
#    conn = psycopg2.connect(
#        f"host={cluster['HOST']} dbname={cluster['DB_NAME']} user={cluster['DB_USER']} password={cluster['DB_PASSWORD']}")
#    cur = conn.cursor()
#    yield cur
#    conn.close()


#@pytest.fixture
#def setup():
#    create_tables.drop_tables()
#    create_tables.create_tables()


#@pytest.fixture
#def teardown():
#    create_tables.drop_tables()


@pytest.fixture(scope="session")
def setup_tables():
    create_tables.drop_tables()
    create_tables.create_tables()
    yield
    create_tables.drop_tables()

def test_staging_events_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').staging_events"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None


def test_staging_songs_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').staging_songs"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None


def test_songplays_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').songplays"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None


def test_songs_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').songs"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None


def test_artists_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').artists"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None


def test_users_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').users"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None


def test_time_table(connection_handler, setup_tables):
    # assert that the table exists
    q_exists = f"SELECT to_regclass('{cluster['DB_NAME']}').time"
    connection_handler.execute(q_exists)
    r = connection_handler.fetchone()
    assert r is not None
