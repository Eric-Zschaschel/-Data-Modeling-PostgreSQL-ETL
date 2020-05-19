# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS artists"
artist_table_drop = "DROP TABLE IF EXISTS songs"
time_table_drop = "DROP TABLE IF EXISTS time"



# CREATE TABLES

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                        start_time TIMESTAMP,
                        user_id INTEGER,
                        level VARCHAR,
                        song_id VARCHAR(64),
                        artist_id VARCHAR(64),
                        session_id INTEGER,
                        location VARCHAR,
                        user_agent VARCHAR,
                        PRIMARY KEY (start_time, user_id),
                        FOREIGN KEY (user_id) REFERENCES users (user_id),
                        FOREIGN KEY (song_id) REFERENCES songs (song_id),
                        FOREIGN KEY (artist_id) REFERENCES artists (artist_id),
                        UNIQUE (start_time, user_id, session_id)
                        );
                        """)

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER,
                    first_name VARCHAR(64),
                    last_name VARCHAR(64),
                    gender CHAR(1),
                    level VARCHAR(32),
                    PRIMARY KEY (user_id)
                    );
                    """)

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                    song_id VARCHAR(64) NOT NULL,
                    title VARCHAR(256),
                    artist_id VARCHAR(64),
                    year INTEGER,
                    duration DOUBLE PRECISION,
                    PRIMARY KEY (song_id),
                    FOREIGN KEY (artist_id) REFERENCES artists (artist_id)
                    );
                    """)

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                    artist_id VARCHAR(64) NOT NULL,
                    name VARCHAR(256),
                    location VARCHAR(256),
                    latitude DOUBLE PRECISION,
                    longitude DOUBLE PRECISION,
                    PRIMARY KEY(artist_id)
                    );
                    """)

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                    start_time TIMESTAMP NOT NULL,
                    hour INTEGER,
                    day INTEGER,
                    week INTEGER,
                    month INTEGER,
                    year INTEGER,
                    weekday INTEGER,
                    PRIMARY KEY (start_time)
                    )
                    """)






# INSERT RECORDS
songplay_table_insert = ("""INSERT INTO songplays (
                        start_time,
                        user_id,
                        level,
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        user_agent
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (start_time, user_id) DO NOTHING;
                        """)

user_table_insert = ("""INSERT INTO users (
                    user_id,
                    first_name,
                    last_name,
                    gender,
                    level
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET level = excluded.level;
                    """)

song_table_insert = ("""INSERT INTO songs (
                    song_id,
                    title,
                    artist_id,
                    year,
                    duration
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id) DO NOTHING;
                    """)

artist_table_insert = ("""INSERT INTO artists (
                    artist_id,
                    name,
                    location,
                    latitude,
                    longitude
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id) DO NOTHING;
                    """)


time_table_insert = ("""INSERT INTO time (
                    start_time,
                    hour,
                    day,
                    week,
                    month,
                    year,
                    weekday
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (start_time) DO NOTHING;
                    """)

# FIND SONGS

song_select = ("""SELECT g.song_id, g.artist_id FROM songs g
            INNER JOIN artists h ON g.artist_id = h.artist_id
            WHERE g.title = %s AND h.name = %s AND g.duration = %s
            """)

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [time_table_drop, songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop]
