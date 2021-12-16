# DROP TABLES

songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    timestamp TIMESTAMP REFERENCES time(timestamp), 
    user_id smallint REFERENCES users(user_id),
    level varchar(4), 
    song_id varchar(18) REFERENCES songs(song_id),
    artist_id varchar(18) REFERENCES artists(artist_id),
    session_id smallint,
    location text,
    userAgent text
    );
CREATE INDEX idx_songplays_ts_and_userid ON songplays (user_id, timestamp DESC);    
""")

user_table_create = ("""
CREATE EXTENSION pg_trgm;
CREATE TABLE IF NOT EXISTS users (
    user_id smallint PRIMARY KEY,
    firstName text,
    lastName text,
    gender varchar(1),
    level varchar(4) 
    );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id varchar(18) PRIMARY KEY,
    title text,
    artist_id varchar(18) REFERENCES artists(artist_id),
    year int,
    duration numeric
    );
CREATE INDEX idx_songs_title ON songs USING gin (title gin_trgm_ops);   
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar(18) PRIMARY KEY,
    artist_name text,
    artist_location text,
    artist_latitude numeric,
    artist_longitude numeric
    );
CREATE INDEX idx_artists_name ON artists USING gin (artist_name gin_trgm_ops);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    timestamp TIMESTAMP PRIMARY KEY,
    hour smallint,
    day smallint,
    weekofyear smallint,
    month smallint,
    year smallint,
    weekday smallint
    );
""")

# INSERT RECORDS

songplay_table_insert = ("""
INSERT INTO songplays ( timestamp,
                        user_id,
                        level, 
                        song_id,
                        artist_id,
                        session_id,
                        location,
                        userAgent
                        )
VALUES (%s, %s, %s, %s,
        %s, %s, %s, %s);
""")

user_table_insert = ("""
INSERT INTO users (user_id, firstName, lastName, gender, level)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (user_id) 
DO UPDATE SET level = excluded.level;
""")

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (song_id) 
DO NOTHING;
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (artist_id) 
DO NOTHING;
""")


time_table_insert = ("""
INSERT INTO time (timestamp, hour, day, weekofyear, month, year, weekday)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (timestamp) 
DO NOTHING;
""")

# FIND SONGS

song_select = ("""
select song_id, songs.artist_id
from songs join artists on songs.artist_id = artists.artist_id
where 
     songs.title = %s and
     artists.artist_name = %s and
     songs.duration = %s;
""")

# QUERY LISTS

# create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
# drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = [user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop]