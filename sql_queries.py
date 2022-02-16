import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXIST time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        num_songs           INTEGER,
        artist_id           BIGINT      NOT NULL,
        artist_latitude     DECIMAL     NOT NULL,
        artist_longitude    DECIMAL     NOT NULL,
        artist_location     TEXT        NOT NULL,
        artist_name         TEXT        NOT NULL,
        song_id             TEXT        NOT NULL,
        title               TEXT        NOT NULL,
        duration            DECIMAL     NOT NULL,
        year                INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        artist              TEXT        NOT NULL,
        auth                TEXT        NOT NULL,
        firstName           TEXT        NOT NULL,
        gender              CHAR,
        itemInSession       INTEGER     NOT NULL,
        lastName            TEXT        NOT NULL,
        lenth               DECIMAL,
        level               TEXT        NOT NULL,
        location            TEXT        NOT NULL,
        method              TEXT        NOT NULL,
        page                TEXT        NOT NULL,
        registration        BIGINT      NOT NULL,
        sessionId           INTEGER     NOT NULL,
        song                TEXT        NOT NULL,
        status              INTEGER     NOT NULL,
        ts                  BIGINT      NOT NULL,
        userAgent           TEXT        NOT NULL,
        userId              BIGINT      NOT NULL
    )
""")

# straying from spec. replacing start_time with time_id for
# relating songplays to time
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        CREATE TABLE songplays (
            songplay_id     IDENTITY(0, 1)      NOT NULL,
            time_id         INTEGER             NOT NULL,
            user_id         BIGINT              NOT NULL,
            level           TEXT                NOT NULL,
            song_id         TEXT,
            artist_id       TEXT,
            session_id      INTEGER, 
            location        TEXT,
            user_agent      TEXT,
            primary key(songplay_id)
        )
        distkey()
        sortkey();
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id         BIGINT          NOT NULL, 
        first_name      TEXT            NOT NULL, 
        last_name       TEXT            NOT NULL, 
        gender          CHAR,
        level           TEXT            NOT NULL,
        primary key(user_id)
    )
    distkey()
    sortkey();
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     TEXT                NOT NULL,
        title       TEXT                NOT NULL,
        artist_id   TEXT                NOT NULL,
        year        INTEGER,
        duration    DECIMAL             NOT NULL,
        primary key(song_id)
    )
    distkey()
    sortkey();
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   TEXT                NOT NULL,
        name        TEXT                NOT NULL,
        location    TEXT,
        latitude    DECIMAL,
        longitude   DECIMAL,
        primary key(artist_id)
    )
    distkey()
    sortkey(name);
""")

# straying from spec. Adding an index column as a primary key.
# primary keys must be unique, start times are not.
# Start_time is an attribute of songplay.
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id     IDENTITY(0, 1),
        start_time  TIMESTAMP           NOT NULL,
        hour        INTEGER             NOT NULL,
        day         INTEGER             NOT NULL,
        week        INTEGER             NOT NULL,
        month       INTEGER             NOT NULL,
        year        INTEGER             NOT NULL,
        weekday     INTEGER             NOT NULL,
        primary key(time_id)
    )
    distkey()
    sortkey(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events
    FROM 's3://udacity-dend/log_data'
    credentials '{}'
    gzip region '{}'
""").format(config['IAM_ROLE']['ARN'], config['AWS']['REGION'])

staging_songs_copy = ("""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data'
    credentials '{}'
    gzip region '{}'
""").format(config['IAM_ROLE']['ARN'], config['AWS']['REGION'])

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay ()
    VALUES ();
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
