import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA = config['S3']['LOG_DATA'] 
SONG_DATA = config['S3']['SONG_DATA'] 
LOG_JSONPATH = config['S3']['LOG_JSONPATH'] 
HOST= config['CLUSTER']['HOST']
DB_NAME= config['CLUSTER']['DB_NAME']
DB_USER= config['CLUSTER']['DB_USER']
DB_PASSWORD= config['CLUSTER']['DB_PASSWORD']
DB_PORT= config['CLUSTER']['DB_PORT']
KEY = config['CLUSTER']['KEY']
SECRET = config['CLUSTER']['SECRET']
ARN= config['IAM_ROLE']['ARN']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= (
    """
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender varchar,
        itemInSession INT,
        lastName varchar,
        length varchar,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration varchar,
        sessionId INT,
        song varchar,
        status INT,
        ts timestamp,
        userAgent varchar,
        userId INT
    ); 
    """
)

staging_songs_table_create = (
    """
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs INT,
        artist_id varchar,
        artist_latitude varchar,
        artist_longitude varchar,
        artist_location varchar,
        artist_name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year INT
    ); 
    """
)

songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT identity(0, 1),
        session_id int NOT NULL,
        location varchar NOT NULL,
        user_agent varchar NOT NULL,
        start_time timestamp,
        user_id int,
        artist_id varchar,
        song_id varchar,
        level varchar
    );    
    """
)

user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id int PRIMARY KEY,
        first_name varchar NOT NULL,
        last_name varchar NOT NULL,
        gender varchar NOT NULL,
        level varchar NOT NULL
    );    
    """
)

song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id varchar PRIMARY KEY,
        title varchar NOT NULL,
        year int NOT NULL,
        duration numeric NOT NULL,
        artist_id varchar
    );
    """
)

artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id varchar PRIMARY KEY,
        name varchar NOT NULL,
        location varchar,
        latitude numeric,
        longitude numeric
    );    
    """
)

time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time timestamp PRIMARY KEY,
        hour int NOT NULL,
        day int NOT NULL,
        week int NOT NULL,
        month int NOT NULL,
        year int NOT NULL,
        weekday int NOT NULL
    );    
    """
)

# STAGING TABLES

staging_events_copy = (
    """
    COPY staging_events FROM {}
    iam_role {}
    json {} compupdate on region 'us-west-2';
    """
).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = (
    """
    COPY staging_songs FROM {}
    iam_role {}
    json 'auto' compupdate on region 'us-west-2';
    """
).format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = (
    """
    INSERT INTO songplays (session_id, location, user_agent, start_time, user_id, artist_id , song_id, level)
    SELECT e.sessionId, e.location, e.userAgent, e.ts, e.userId, a.artist_id,
    s.song_id, e.level
    FROM staging_events e
    LEFT OUTER JOIN artists a ON a.artist = e.artist
    LEFT OUTER JOIN songs s ON s.title = e.song
    """
)

user_table_insert = (
    """
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page= 'NextSong' AND user_id NOT IN (SELECT DISTINCT user_id FROM users)
    """
)

song_table_insert = (
    """
    INSERT INTO songs (song_id, title, year, duration, artist_id) 
    SELECT DISTINCT song_id, title, year, duration, artist_id
    FROM staging_songs
    WHERE song_id NOT IN (SELECT DISTINCT song_id FROM songs)    
    """
)

artist_table_insert = (
    """
    INSERT INTO artists (artist_id, name, location, latitude, longitude) 
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
    """
)

time_table_insert = (
    """
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT a.start_time, EXTRACT(hour FROM a.start_time), 
    EXTRACT(day FROM a.start_time), EXTRACT(week FROM a.start_time),
    EXTRACT(month FROM a.start_time), EXTRACT(year FROM a.start_time),
    EXTRACT(weekday FROM a.start_time)
    FROM
    (SELECT timestamp'epoch' + start_time/1000 * INTERVAL '1 second' as start_time FROM songplays) a;
    """
)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
