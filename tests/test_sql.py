import psycopg2
from psycopg2 import extensions
import pytest
from string import Template

# build a test database
# create a table
# run the query where the query is a templated string with double dollar quoted stuff


@pytest.fixture
def connection_handler():
    # ensure scratchdb exists first, then return a connection to a database session with it
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=postgres user=student password=student")
    conn.set_isolation_level(extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("DROP DATABASE IF EXISTS scratchdb;")
    cur.execute("CREATE DATABASE scratchdb;")
    conn.close()

    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=scratchdb user=student password=student")
    cur = conn.cursor()
    cur.execute(song_table_create)
    yield conn

    # tear down
    cur.execute("DROP TABLE IF EXISTS songs")
    conn.close()


# @pytest.fixture
# def create_db(connection_handler):
#    cur = connection_handler.cursor()
#    cur.execute("DROP DATABASE IF EXISTS scratchdb;")
#    cur.execute("CREATE DATABASE scratchdb;")
#    # WITH ENCODING 'utf8' TEMPLATE template0
#    connection_handler.close()
#    connection_handler = psycopg2.connect("host=127.0.0.1 dbname=scratchdb user=student password=student")


# @pytest.fixture
# def create_table(connection_handler):
#    cur = connection_handler.cursor()
#    cur.execute(song_table_create)


def test_double_quoted_string_with_template(connection_handler):
    cur = connection_handler.cursor()
    try:
        q = Template(song_table_insert).substitute(
            title="Fair Chance",
            artist="Thundercat, Ty Dolla $ign, Lil B",
            year="2020",
            duration="237.00")
        cur.execute(q)
        cur.execute("SELECT * FROM songs;")
        row = cur.fetchone()
        assert row[0] == "Fair Chance"
        assert row[1] == "Thundercat, Ty Dolla $ign, Lil B"
        assert row[2] == 2020
        assert row[3] == 237
    except Exception as e:
        assert pytest.fail(e)


song_table_create = f"""
    CREATE TABLE IF NOT EXISTS songs
        (
            title     VARCHAR NOT NULL,
            artist    VARCHAR NOT NULL,
            year      INT,
            duration  NUMERIC NOT NULL
  );
"""

song_table_insert = f"""
    INSERT INTO songs 
        (
            title, 
            artist, 
            year, 
            duration
        )
    VALUES 
        (
            $$t$$$title$$t$$, 
            $$a$$$artist$$a$$, 
            $$y$$$year$$y$$, 
            $$d$$$duration$$d$$
        );
"""
