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
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist              TEXT,
        auth                TEXT,
        firstName           TEXT,
        gender              CHAR,
        itemInSession       INTEGER,
        lastName            TEXT,
        lenth               DECIMAL,
        level               TEXT,
        location            TEXT,
        method              TEXT,
        page                TEXT,
        registration        BIGINT,
        sessionId           INTEGER,
        song                TEXT,
        status              INTEGER,
        ts                  BIGINT,
        userAgent           TEXT,
        userId              BIGINT
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INTEGER,
        artist_id           TEXT,
        artist_latitude     DECIMAL,
        artist_longitude    DECIMAL,
        artist_location     TEXT,
        artist_name         TEXT,
        song_id             TEXT,
        title               TEXT,
        duration            DECIMAL,
        year                INTEGER
        
    )
""")

# straying from spec. replacing start_time with time_id for
# relating songplays to time
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     BIGINT              IDENTITY(0,1),
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
    distkey(songplay_id)
    sortkey(time_id);
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
    distkey(user_id)
    sortkey(last_name);
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
    distkey(song_id)
    sortkey(title);
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
    distkey(artist_id)
    sortkey(name);
""")

# straying from spec. Adding an index column as a primary key.
# primary keys must be unique, start times are not.
# Start_time is an attribute of songplay.
time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        time_id     BIGINT              IDENTITY(0,1),
        start_time  TIMESTAMP           NOT NULL,
        hour        INTEGER             NOT NULL,
        day         INTEGER             NOT NULL,
        week        INTEGER             NOT NULL,
        month       INTEGER             NOT NULL,
        year        INTEGER             NOT NULL,
        weekday     INTEGER             NOT NULL,
        primary key(time_id)
    )
    distkey(month)
    sortkey(start_time);
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events
    FROM 's3://udacity-dend/log_data/'
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    JSON 's3://udacity-dend/log_json_path.json'
    REGION '{config['AWS']['REGION']}';
""")

staging_songs_copy = (f"""
    COPY staging_songs
    FROM 's3://udacity-dend/song_data/'
    CREDENTIALS 'aws_iam_role={config['IAM_ROLE']['ARN']}'
    JSON 'auto'
    REGION '{config['AWS']['REGION']}';
""")

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
