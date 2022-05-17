songplay_table_create = """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     SERIAL              PRIMARY KEY,
        time_id         INTEGER             NOT NULL,
        user_id         BIGINT              NOT NULL,
        level           TEXT                NOT NULL,
        song_id         TEXT,
        artist_id       TEXT,
        session_id      INTEGER, 
        location        TEXT,
        user_agent      TEXT
    );
"""

user_table_create = """
    CREATE TABLE IF NOT EXISTS users (
        user_id         BIGINT          PRIMARY KEY, 
        first_name      TEXT            NOT NULL, 
        last_name       TEXT            NOT NULL, 
        gender          CHAR,
        level           TEXT            NOT NULL
    );
"""

song_table_create = """
    CREATE TABLE IF NOT EXISTS songs (
        song_id     TEXT                PRIMARY KEY,
        title       TEXT                NOT NULL,
        artist_id   TEXT                NOT NULL,
        year        INTEGER,
        duration    DECIMAL(9, 5)       NOT NULL
    );
"""

artist_table_create = """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   TEXT                PRIMARY KEY,
        name        TEXT                NOT NULL,
        location    TEXT,
        latitude    DECIMAL(5, 2),
        longitude   DECIMAL(5, 2)
    );
"""

time_table_create = """
    CREATE TABLE IF NOT EXISTS time (
        time_id     SERIAL              PRIMARY KEY,
        start_time  TIMESTAMP           NOT NULL,
        hour        INTEGER             NOT NULL,
        day         INTEGER             NOT NULL,
        week        INTEGER             NOT NULL,
        month       INTEGER             NOT NULL,
        year        INTEGER             NOT NULL,
        weekday     INTEGER             NOT NULL
    );
"""