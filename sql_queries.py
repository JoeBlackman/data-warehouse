import config
#import configparser


# CONFIG
#config = configparser.ConfigParser()
#config.read('dwh.cfg')

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
        lenth               DECIMAL(9, 5),
        level               TEXT,
        location            TEXT,
        method              TEXT,
        page                TEXT,
        registration        VARCHAR(15),
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
        artist_latitude     DECIMAL(5, 2),
        artist_longitude    DECIMAL(5, 2),
        artist_location     VARCHAR(512),
        artist_name         VARCHAR(512),
        song_id             TEXT,
        title               VARCHAR(512),
        duration            DECIMAL(9, 5),
        year                INTEGER
    )
""")

# straying from spec. replacing start_time with time_id for
# relating songplays to time
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     BIGINT              IDENTITY(0,1),
        start_time      TIMESTAMP           NOT NULL,
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
    sortkey(start_time);
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
        duration    DECIMAL(9, 5)       NOT NULL,
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
        latitude    DECIMAL(5, 2),
        longitude   DECIMAL(5, 2),
        primary key(artist_id)
    )
    distkey(artist_id)
    sortkey(name);
""")


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP           NOT NULL,
        hour        INTEGER             NOT NULL,
        day         INTEGER             NOT NULL,
        week        INTEGER             NOT NULL,
        month       INTEGER             NOT NULL,
        year        INTEGER             NOT NULL,
        weekday     INTEGER             NOT NULL,
        primary key(start_time)
    )
    distkey(month)
    sortkey(start_time);
""")

# STAGING TABLES

staging_events_copy = (f"""
    COPY staging_events
    FROM '{config.S3_LOG_DATA}'
    CREDENTIALS 'aws_iam_role={config.IAM_ROLE_ARN}'
    JSON '{config.S3_LOG_JSON_PATH}'
    REGION '{config.REGION}';
""")

staging_songs_copy = (f"""
    COPY staging_songs
    FROM '{config.S3_SONG_DATA}'
    CREDENTIALS 'aws_iam_role={config.IAM_ROLE_ARN}'
    JSON 'auto'
    REGION '{config.REGION}';
""")

# FINAL TABLES

# if this uses time_id instead of start_time, order of execution matters
# i don't like the potential for non-unique timestamps but 
# if all of the logic for handling this kind of stuff is supposed to be
# done in the sql statements, i'm inclined to just let time non-uniqueness happen
# on the other hand, lookup for song_id, artist_id foreign keys already necessary,
# might as well do a lookup of the time_id anyways
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        WITH uniq_staging_events AS (
            SELECT (timestamp 'epoch' + se.ts::numeric / 1000 * interval '1 second') as start_time, se.userId, ss.song_id, 
                ss.artist_id, se.level, se.song, se.artist, se.sessionId, se.location, se.userAgent
            FROM staging_events se
            LEFT OUTER JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name
            WHERE se.page = 'NextSong'
        )
        SELECT start_time, userId, level, song_id, artist_id, sessionId, location, userAgent
        FROM uniq_staging_events;
""")

# we need to pre-emptively handle duplicates instead of cleaning them up after insertion
# - going to do this by way of using a temporary table
#   - temporary table will keep only one instance of an entity for each entity that has duplicates
#   - choice of which entity to keep will be based on latest timestamp
# - common table expression used here, defines a temporary table
#   - ignore events where no user is logged in
#   - choice of which entity to keep will be based on latest timestamp
#       - ordered by timestamp descending so latest timestamp will make rank equal to 1
#       - only rows with rank == 1 will be inserted (to prevent duplicates)
#   - window function assigns a sequential integer to each row in a result set
#       - row_number() OVER(PARTITION BY column1, column2 ORDER BY column3, column4 DESC)
user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
        WITH uniq_staging_events AS (
            SELECT userId, firstName, lastName, gender, level, ROW_NUMBER() OVER(PARTITION BY userId ORDER BY ts DESC) AS rank
            FROM staging_events
            WHERE userId IS NOT NULL
        )
        SELECT userId, firstName, lastName, gender, level
        FROM uniq_staging_events
        WHERE rank = 1;
""")

# in the event of a duplicate song, do we care about which one gets selected? (no?)
# basically same process as users except most recent record doesn't matter
# first record found will get rank one and we'll still use rank to maintain uniqueness
song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
        WITH uniq_staging_songs AS (
            SELECT song_id, title, artist_id, year, duration, ROW_NUMBER() OVER(PARTITION BY song_id) AS rank
            FROM staging_songs
        )
        SELECT song_id, title, artist_id, year, duration
        FROM uniq_staging_songs
        WHERE rank = 1;
""")

# built from the staging songs table, need to make a temp table to reduce for uniqueness
artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longitude)
        WITH uniq_staging_songs AS (
            SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, 
            artist_longitude AS longitude, ROW_NUMBER() OVER(PARTITION BY artist_id) AS rank
            FROM staging_songs
        )
        SELECT artist_id, name, location, latitude, longitude
        FROM uniq_staging_songs
        WHERE rank = 1;
""")

# need to break out timestamp into calculated fields. can this be done in sql?
time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
        WITH uniq_start_times AS (
            SELECT (timestamp 'epoch' + ts::numeric / 1000 * interval '1 second') as start_time, ROW_NUMBER() OVER(PARTITION BY ts) AS rank
		    FROM public.staging_events
        )
        SELECT start_time, EXTRACT(HOUR FROM start_time) AS hour, EXTRACT(DAY FROM start_time) AS day, EXTRACT(WEEK FROM start_time) AS week, 
	        EXTRACT(MONTH FROM start_time) AS month, EXTRACT(YEAR FROM start_time) AS year, EXTRACT(DOW FROM start_time) AS weekday
        FROM uniq_start_times
        WHERE rank = 1;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
