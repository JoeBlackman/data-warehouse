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

# ANALYSIS TABLES
# NOTE: when these templated strings are passed into string.template and
# the substitute function is called on them,
# '$$' is considered an escape and is replaced with '$',
# '$identifier' will be replaced by args from the substitute function
# this is necessary to create templated strings that hold
# double dollar quotes in postgres sql query

songplay_table_insert = f"""
    INSERT INTO songplays 
        (
            time_id, 
            user_id, 
            level, 
            song_id, 
            artist_id, 
            session_id, 
            location, 
            user_agent
        )
    VALUES 
        (
            $$time_id$$$time_id$$time_id$$, 
            $$user_id$$$user_id$$user_id$$, 
            $$level$$$level$$level$$, 
            $$song_id$$$song_id$$song_id$$,
            $$artist_id$$$artist_id$$artist_id$$, 
            $$session_id$$$session_id$$session_id$$, 
            $$location$$$location$$location$$, 
            $$user_agent$$$user_agent$$user_agent$$
        );
"""

user_table_insert = f"""
    INSERT INTO users
        (
            user_id,
            first_name,
            last_name,
            gender,
            level
        )
    VALUES 
        (
            $$user_id$$$user_id$$user_id$$, 
            $$first_name$$$first_name$$first_name$$, 
            $$last_name$$$last_name$$last_name$$, 
            $$gender$$$gender$$gender$$, 
            $$level$$$level$$level$$
        );
"""

song_table_insert = f"""
    INSERT INTO songs 
        (
            song_id, 
            title, 
            artist_id, 
            year, 
            duration
        )
    VALUES 
        (
            $$song_id$$$song_id$$song_id$$, 
            $$title$$$title$$title$$, 
            $$artist_id$$$artist_id$$artist_id$$, 
            $$year$$$year$$year$$, 
            $$duration$$$duration$$duration$$
        );
"""

artist_table_insert = f"""
    INSERT INTO artists
        (
            artist_id,
            name,
            location,
            latitude,
            longitude
        )
    VALUES
        (
            $$artist_id$$$artist_id$$artist_id$$, 
            $$name$$$name$$name$$, 
            $$location$$$location$$location$$, 
            $$latitude$$$latitude$$latitude$$, 
            $$longitude$$$longitude$$longitude$$
        );
"""

time_table_insert = f"""
    INSERT INTO time
        (
            start_time,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
    VALUES
        (
            $$start_time$$$start_time$$start_time$$, 
            $$hour$$$hour$$hour$$, 
            $$day$$$day$$day$$, 
            $$week$$$week$$week$$,
            $$month$$$month$$month$$,
            $$year$$$year$$year$$,
            $$weekday$$$weekday$$weekday$$
        );
"""

# source: https://elliotchance.medium.com/removing-duplicate-data-in-redshift-45a43b7ae334
clean_up_duplicate_songs = """
BEGIN
-- Identify duplicates
CREATE TEMP TABLE dup_songs AS
SELECT song_id
FROM songs
GROUP BY song_id
HAVING COUNT(*) >1;

-- Extract one copy of each duplicate
CREATE TEMP TABLE new_songs(LIKE songs);
INSERT INTO new_songs
SELECT DISTINCT *
FROM songs
WHERE song_id IN (SELECT song_id FROM dup_songs);

-- Remove all rows that were duplicated (all copies)
DELETE FROM songs
WHERE song_id IN (SELECT song_id FROM dup_songs);

-- Insert the rest of the records that had no duplicates
INSERT INTO songs
SELECT *
FROM new_songs;

-- clean up
DROP TABLE dup_songs;
DROP TABLE new_songs;
COMMIT;
"""

clean_up_duplicate_artists = """
BEGIN
-- Identify duplicates
CREATE TEMP TABLE dup_artists AS
SELECT artist_id
FROM artists
GROUP BY artist_id
HAVING COUNT(*) >1;

-- Extract one copy of each duplicate
CREATE TEMP TABLE new_artists(LIKE artists);
INSERT INTO new_artists
SELECT DISTINCT *
FROM artists
WHERE artist_id IN (SELECT artist_id FROM dup_artists);

-- Remove all rows that were duplicated (all copies)
DELETE FROM artists
WHERE artist_id IN (SELECT artist_id FROM dup_artists);

-- Insert the rest of the records that had no duplicates
INSERT INTO artists
SELECT *
FROM new_artists;

-- clean up
DROP TABLE dup_artists;
DROP TABLE new_artists;
COMMIT;
"""

clean_up_duplicate_users = """
BEGIN
-- Identify duplicates
CREATE TEMP TABLE dup_users AS
SELECT user_id
FROM users
GROUP BY user_id
HAVING COUNT(*) >1;

-- Extract one copy of each duplicate
CREATE TEMP TABLE new_users(LIKE users);
INSERT INTO new_users
SELECT DISTINCT *
FROM users
WHERE user_id IN (SELECT user_id FROM dup_users);

-- Remove all rows that were duplicated (all copies)
DELETE FROM users
WHERE user_id IN (SELECT user_id FROM dup_users);

-- Insert the rest of the records that had no duplicates
INSERT INTO users
SELECT *
FROM new_users;

-- clean up
DROP TABLE dup_users;
DROP TABLE new_users;
COMMIT;
"""

get_last_time_id = f"""
    SELECT time_id
    FROM time
    ORDER BY time_id
    LIMIT 1;
"""

get_artist_id = f"""
    SELECT artist_id
    FROM artists
    WHERE name=$$n$$$name$$n$$;
"""

get_song_id = f"""
    SELECT song_id
    FROM songs
    WHERE title=$$t$$$title$$t$$ AND artist_id=$$a$$$artist_id$$a$$;
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
