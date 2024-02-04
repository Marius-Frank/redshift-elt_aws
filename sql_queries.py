import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# Create separate schemas for staging and analytics tables
staging_schema_create   = "CREATE SCHEMA IF NOT EXISTS staging;"
analytics_schema_create = "CREATE SCHEMA IF NOT EXISTS analytics;"

# Drop separate schemas for staging and analytics tables
staging_schema_drop   = "DROP SCHEMA IF EXISTS staging;"
analytics_schema_drop = "DROP SCHEMA IF EXISTS analytics;"


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging.events;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging.songs;"
songplay_table_drop       = "DROP TABLE IF EXISTS analytics.songplays;"
user_table_drop           = "DROP TABLE IF EXISTS analytics.users;"
song_table_drop           = "DROP TABLE IF EXISTS analytics.songs;"
artist_table_drop         = "DROP TABLE IF EXISTS analytics.artists;"
time_table_drop           = "DROP TABLE IF EXISTS analytics.time;"

# CREATE TABLES

staging_events_table_create = """
CREATE TABLE IF NOT EXISTS staging.events 
(
    artist VARCHAR(128),
    auth VARCHAR(32),
    firstName VARCHAR(64),
    gender VARCHAR(2),
    itemInSession INTEGER,
    lastName VARCHAR(64),
    length FLOAT,
    level VARCHAR(32),
    location VARCHAR(128),
    method VARCHAR(32),
    page VARCHAR(32),
    registration REAL,
    sessionId INTEGER,
    song VARCHAR(256),
    status SMALLINT,
    ts BIGINT,
    userAgent VARCHAR(256),
    userId INTEGER
);
"""

staging_songs_table_create = """
CREATE TABLE IF NOT EXISTS staging.songs
(
    num_songs SMALLINT,
    artist_id VARCHAR(18),
    artist_latitude REAL,
    artist_longitude REAL,
    artist_location VARCHAR(128),
    artist_name VARCHAR(128),
    song_id VARCHAR(18),
    title VARCHAR(256),
    duration REAL,
    year SMALLINT
);
"""

songplay_table_create = """
CREATE TABLE IF NOT EXISTS analytics.songplays
(
    songplay_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    start_time TIMESTAMP,
    user_id INTEGER,
    level VARCHAR(32),
    song_id VARCHAR(18),
    artist_id VARCHAR(18),
    session_id INTEGER,
    location VARCHAR(128),
    user_agent VARCHAR(256)
);
"""

user_table_create = """
CREATE TABLE IF NOT EXISTS analytics.users
(
    user_id INTEGER PRIMARY KEY,
    first_name VARCHAR(64),
    last_name VARCHAR(64),
    gender VARCHAR(2),
    level VARCHAR(32)
);
"""

song_table_create = """
CREATE TABLE IF NOT EXISTS analytics.songs
(
    song_id VARCHAR(18) PRIMARY KEY,
    title VARCHAR(256),
    artist_id VARCHAR(18),
    year SMALLINT,
    duration REAL
);
"""

artist_table_create = """
CREATE TABLE IF NOT EXISTS analytics.artists
(
    artist_id VARCHAR(18) PRIMARY KEY,
    artist_name VARCHAR(128),
    artist_location VARCHAR(128),
    artist_latitude REAL,
    artist_longitude REAL
);
"""

time_table_create = """
CREATE TABLE IF NOT EXISTS analytics.time
(
    start_time TIMESTAMP PRIMARY KEY,
    hour SMALLINT NOT NULL,
    day SMALLINT NOT NULL,
    week SMALLINT NOT NULL,
    month SMALLINT NOT NULL,
    year SMALLINT NOT NULL,
    weekday SMALLINT NOT NULL
);
"""

# STAGING TABLES

staging_events_copy = """
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON {} region '{}';
""".format(
    'staging.events',
    config['S3']['LOG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['S3']['LOG_JSONPATH'],
    config['REGION']['REGION_NAME'])

staging_songs_copy = """
    COPY {} FROM {}
    IAM_ROLE '{}'
    JSON 'auto ignorecase' region '{}';
""".format(
    'staging.songs',
    config['S3']['SONG_DATA'],
    config['IAM_ROLE']['ARN'],
    config['REGION']['REGION_NAME'])


# FINAL TABLES (INSERT STATEMENTS WERE PARTIALLY INSPIRED BY https://knowledge.udacity.com/questions/960055)

songplay_table_insert = """
INSERT INTO analytics.songplays (start_time, user_id, level, song_id, artist_id, session_id,
                                location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second',
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent
FROM staging.events e
LEFT JOIN staging.songs s
ON e.song = s.title AND e.artist = s.artist_name
   AND ABS(e.length - s.duration) < 5
WHERE page = 'NextSong'
AND e.userId IS NOT NULL
AND s.song_id IS NOT NULL
AND s.artist_ID IS  NOT NULL
AND e.ts IS NOT NULL;
"""

user_table_insert = """
INSERT INTO analytics.users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging.events
WHERE userId is NOT NULL;
"""

song_table_insert = """
INSERT INTO analytics.songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging.songs
WHERE song_id IS NOT NULL;
"""

artist_table_insert = """
INSERT INTO analytics.artists (artist_id, artist_name, artist_location,
                               artist_latitude, artist_longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, 
                artist_latitude, artist_longitude
FROM staging.songs
WHERE artist_id IS NOT NULL;
"""

time_table_insert = """
INSERT INTO analytics.time
WITH date_table AS (
    SELECT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS date
    FROM staging.events
)
SELECT date,
       EXTRACT(HOUR FROM date),
       EXTRACT(DAY FROM date),
       EXTRACT(WEEK FROM date),
       EXTRACT(MONTH FROM date),
       EXTRACT(YEAR FROM date),
       EXTRACT(DOW FROM date)
FROM date_table
WHERE date IS NOT NULL;
"""

# analytics.time (start_time, hour, day, week, month, year, weekday)

# QUERY LISTS
create_schema_queries = [analytics_schema_create, staging_schema_create]
drop_schema_queries = [analytics_schema_drop, staging_schema_drop]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
