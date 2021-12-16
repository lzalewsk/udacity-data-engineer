import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN             = config.get('IAM_ROLE', 'ARN')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS ft_songplays"
user_table_drop = "DROP TABLE IF EXISTS dt_users"
song_table_drop = "DROP TABLE IF EXISTS dt_songs"
artist_table_drop = "DROP TABLE IF EXISTS dt_artists"
time_table_drop = "DROP TABLE IF EXISTS dt_time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
                event_id    BIGINT IDENTITY(0,1)    NULL,
                artist      VARCHAR                 NULL,
                auth        VARCHAR                 NULL,
                firstName   VARCHAR                 NULL,
                gender      VARCHAR                 NULL,
                itemInSession INTEGER               NULL,
                lastName    VARCHAR                 NULL,
                length      DECIMAL(9)              NULL,
                level       VARCHAR                 NULL,
                location    VARCHAR                 NULL,
                method      VARCHAR                 NULL,
                page        VARCHAR                 NULL,
                registration VARCHAR                NULL,
                sessionId   INTEGER                 NULL SORTKEY DISTKEY,
                song        VARCHAR                 NULL,
                status      INTEGER                 NULL,
                ts          BIGINT                  NULL,
                userAgent   VARCHAR                 NULL,
                userId      INTEGER                 NULL
    );
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs           INTEGER         NULL,
                artist_id           VARCHAR         NULL SORTKEY DISTKEY,
                artist_latitude     VARCHAR         NULL,
                artist_longitude    VARCHAR         NULL,
                artist_location     VARCHAR(500)    NULL,
                artist_name         VARCHAR(500)    NULL,
                song_id             VARCHAR         NULL,
                title               VARCHAR(500)    NULL,
                duration            DECIMAL(9)      NULL,
                year                INTEGER         NULL
    );
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS ft_songplays (
                songplay_id INTEGER IDENTITY(0,1)   PRIMARY KEY SORTKEY,
                start_time  TIMESTAMP               NOT NULL REFERENCES dt_time(start_time),
                user_id     SMALLINT                NOT NULL REFERENCES dt_users(user_id),
                level       VARCHAR(4)              NOT NULL,
                song_id     VARCHAR(18)             NOT NULL REFERENCES dt_songs(song_id) DISTKEY,
                artist_id   VARCHAR(18)             NOT NULL REFERENCES dt_artists(artist_id),
                session_id  SMALLINT                NOT NULL,
                location    TEXT                    NULL,
                user_agent  TEXT                    NULL
    );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS dt_users (
                user_id     SMALLINT                PRIMARY KEY SORTKEY,
                first_name  TEXT                    NULL,
                last_name   TEXT                    NULL,
                gender      VARCHAR(1)              NULL,
                level       VARCHAR(4)              NULL
    ) diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS dt_songs (
                song_id     VARCHAR(18)             PRIMARY KEY DISTKEY SORTKEY,
                title       TEXT                    NOT NULL,
                artist_id   VARCHAR(18)             NOT NULL REFERENCES dt_artists(artist_id),
                year        INTEGER                 NOT NULL,
                duration    DECIMAL(9)              NOT NULL
    );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS dt_artists (
                artist_id   VARCHAR(18)             PRIMARY KEY SORTKEY,
                name        TEXT                    NULL,
                location    TEXT                    NULL,
                latitude    DECIMAL(9)              NULL,
                longitude   DECIMAL(9)              NULL
    ) diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS dt_time (
                start_time  TIMESTAMP               PRIMARY KEY SORTKEY,
                hour        SMALLINT                NULL,
                day         SMALLINT                NULL,
                week        SMALLINT                NULL,
                month       SMALLINT                NULL,
                year        SMALLINT                NULL,
                weekday     SMALLINT                NULL
    ) diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO ft_songplays (start_time,
                           user_id,
                           level,
                           song_id,
                           artist_id,
                           session_id,
                           location,
                           user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 \
                * INTERVAL '1 second'   AS start_time,
            se.userId                   AS user_id,
            se.level                    AS level,
            ss.song_id                  AS song_id,
            ss.artist_id                AS artist_id,
            se.sessionId                AS session_id,
            se.location                 AS location,
            se.userAgent                AS user_agent
    FROM staging_events AS se
    JOIN staging_songs AS ss
        ON (se.artist = ss.artist_name AND se.length = ss.duration)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO dt_users (user_id,
                       first_name,
                       last_name,
                       gender,
                       level)
    SELECT  DISTINCT userId          AS user_id,
            firstName                AS first_name,
            lastName                 AS last_name,
            gender                   AS gender,
            level                    AS level
    FROM staging_events
    WHERE userId is NOT NULL
    and page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO dt_songs (song_id,
                       title,
                       artist_id,
                       year,
                       duration)
    SELECT  DISTINCT song_id         AS song_id,
            title                    AS title,
            artist_id                AS artist_id,
            year                     AS year,
            duration                 AS duration
    FROM staging_songs;
""")

artist_table_insert = ("""
    INSERT INTO dt_artists (artist_id,
                         name,
                         location,
                         latitude,
                         longitude)
    SELECT  DISTINCT artist_id       AS artist_id,
            artist_name              AS name,
            artist_location          AS location,
            artist_latitude          AS latitude,
            artist_longitude         AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
    INSERT INTO dt_time (start_time,
                      hour,
                      day,
                      week,
                      month,
                      year,
                      weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time)    AS hour,
            EXTRACT(day FROM start_time)     AS day,
            EXTRACT(week FROM start_time)    AS week,
            EXTRACT(month FROM start_time)   AS month,
            EXTRACT(year FROM start_time)    AS year,
            EXTRACT(week FROM start_time)    AS weekday
    FROM    staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

# create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, time_table_create, song_table_create, songplay_table_create]

# drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_events_copy, staging_songs_copy]

# insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
insert_table_queries = [user_table_insert, artist_table_insert, time_table_insert, song_table_insert, songplay_table_insert]