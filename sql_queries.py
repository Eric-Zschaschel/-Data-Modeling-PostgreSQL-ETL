# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"
staging_songs_drop = "DROP TABLE IF EXISTS staging_songs"
staging_events_drop = "DROP TABLE IF EXISTS staging_events"


# CREATE TABLES
staging_songs_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (
                        song_id VARCHAR(64),
                        title VARCHAR(256),
                        artist_id VARCHAR(64),
                        year INTEGER,
                        duration DOUBLE PRECISION,
                        name VARCHAR(256),
                        location VARCHAR(256),
                        latitude DOUBLE PRECISION,
                        longitude DOUBLE PRECISION
                        );
                        """)

staging_events_create = ("""CREATE TABLE IF NOT EXISTS staging_events (
                        start_time TIMESTAMP,
                        user_id INTEGER,
                        level VARCHAR(32),
                        session_id INTEGER,
                        location VARCHAR,
                        user_agent VARCHAR,
                        first_name VARCHAR(64),
                        last_name VARCHAR(64),
                        gender CHAR(1),
                        song VARCHAR,
                        artist VARCHAR,
                        length DOUBLE PRECISION
                        );
                        """)


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


#INSERT STATEMENTS FASTER
artists_insert = ("""INSERT INTO artists (
                    artist_id, name, location, latitude, longitude)
                    SELECT artist_id, name, location, latitude,
                    longitude FROM staging_songs
                    ON CONFLICT (artist_id) DO NOTHING;""")

songs_insert = ("""INSERT INTO songs (
                song_id, title, artist_id, year, duration)
                SELECT song_id, title, artist_id, year, duration
                FROM staging_songs
                ON CONFLICT (song_id) DO NOTHING;""")

time_insert = ("""INSERT INTO time (
                start_time, hour, day, week, month, year, weekday)
                SELECT
                start_time,
                EXTRACT(hour FROM start_time),
                EXTRACT(day FROM start_time),
                EXTRACT(week FROM start_time),
                EXTRACT(month FROM start_time),
                EXTRACT(year FROM start_time),
                EXTRACT(dow FROM start_time)
                FROM staging_events
                ON CONFLICT (start_time) DO NOTHING;""")

users_insert = ("""INSERT INTO users (
                user_id, first_name, last_name, gender, level)
                SELECT user_id, first_name, last_name, gender, level
                FROM staging_events
                WHERE user_id IS NOT NULL AND user_id <> 0
                ON CONFLICT (user_id) DO NOTHING;""")

songplays_insert = ("""INSERT INTO songplays (start_time, user_id, level,
                song_id, artist_id, session_id, location, user_agent)
                SELECT g.start_time, g.user_id, g.level, h.song_id,
                h.artist_id, g.session_id, g.location, g.user_agent
                FROM staging_events g
                LEFT JOIN (
                    SELECT song_id, j.artist_id, k.name as artist, title,
                    duration FROM songs j INNER JOIN artists k
                    ON j.artist_id = k.artist_id) h ON g.song = h.title
                    AND g.length = h.duration AND g.artist = h.artist
                ON CONFLICT (start_time, user_id) DO NOTHING;""")


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
insert_queries = [artists_insert, songs_insert, time_insert, users_insert, songplays_insert]
create_table_queries = [staging_songs_create, staging_events_create, user_table_create, artist_table_create, song_table_create, songplay_table_create, time_table_create]
drop_table_queries = [staging_songs_drop, staging_events_drop, time_table_drop, songplay_table_drop, song_table_drop, artist_table_drop, user_table_drop]
