
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artist "
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES 

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
artist          VARCHAR,
auth            VARCHAR, 
firstName       VARCHAR,
gender          VARCHAR,   
itemInSession   INTEGER,
lastName        VARCHAR,
length          FLOAT,
level           VARCHAR, 
location        VARCHAR,
method          VARCHAR,
page            VARCHAR,
registration    BIGINT,
sessionId       INTEGER,
song            VARCHAR,
status          INTEGER,
ts              TIMESTAMP,
userAgent       VARCHAR,
userId          INTEGER
);
""")

staging_songs_table_create = ("""CREATE TABLE staging_songs 
(num_songs INTEGER,
artist_id VARCHAR(40),
artist_latitude VARCHAR(50),
artist_longitude VARCHAR(50),
artist_location VARCHAR(50),
artist_name VARCHAR(50),
song_id VARCHAR(50),
title VARCHAR(50),
duration NUMERIC(15,5),
year INTEGER
)
""")

songplay_table_create = ("""CREATE TABLE songplay
(
songplayId INTEGER IDENTITY (1, 1) PRIMARY KEY,
startTime TIMESTAMP REFERENCES time (startTime),
userId INTEGER REFERENCES users (user_id) ,
level VARCHAR(40) ,
songId VARCHAR(50) REFERENCES song (songId),
artistId VARCHAR(40) REFERENCES artist (artistId),
sessionId INTEGER,
location VARCHAR(200),
userAgent VARCHAR(300)
)
""")

user_table_create = ("""CREATE TABLE users
(
user_id INTEGER PRIMARY KEY,
firstName VARCHAR(30),
lastName VARCHAR(30),
gender VARCHAR(8),
level VARCHAR(40)
)
""")

song_table_create = ("""CREATE TABLE song 
(
songId VARCHAR(50) PRIMARY KEY,
title VARCHAR(50),
artistId VARCHAR(40) UNIQUE,
year INTEGER,
duration NUMERIC(15,5)
)
""")

artist_table_create = ("""CREATE TABLE artist 
(
artistId VARCHAR(40) PRIMARY KEY REFERENCES song (artistId) ,
name VARCHAR(50),
location VARCHAR(200),
latitude VARCHAR(50),
longitude VARCHAR(50)
)
""")

time_table_create = ("""CREATE TABLE time 
(
startTime TIMESTAMP PRIMARY KEY,
hour INTEGER,
day INTEGER,
week INTEGER,
month INTEGER,
year INTEGER,
weedDay INTEGER
)
""")

# STAGING TABLES
LOG_DATA = config.get('S3',"LOG_DATA")
ARN = config.get('IAM_ROLE',"ARN")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")


staging_events_copy = ("""
    COPY staging_events FROM {}
    CREDENTIALS 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    TIMEFORMAT as 'epochmillisecs'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
    FORMAT AS JSON {}
""").format(LOG_DATA, ARN, LOG_JSONPATH)



SONG_DATA = config.get("S3","SONG_DATA")
staging_songs_copy = ("""
COPY staging_songs FROM {}
    CREDENTIALS 'aws_iam_role={}'
    compupdate off region 'us-west-2'
    FORMAT AS JSON 'auto'
    TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL;
""").format(SONG_DATA,ARN )

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (startTime,userId,level,songId,artistId,sessionId,location,userAgent)
SELECT e.ts,
       e.userId,
       e.level,
       s.song_id,
       s.artist_id,
       e.sessionId,
       e.location,
       e.userAgent
       FROM 
staging_events e, staging_songs s
WHERE e.artist = s.artist_name
""")

user_table_insert = ("""INSERT INTO users 
SELECT DISTINCT userId ,
                firstName,
                lastName,
                gender,
                level
                from staging_events
                WHERE userId IS NOT NULL
                
""")

song_table_insert = ("""
INSERT INTO song
SELECT DISTINCT song_id,
                title,
                artist_id,
                year,
                duration
                from staging_songs
                where song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artist
SELECT DISTINCT artist_id,
                artist_name,
                artist_location,
                artist_latitude,
                artist_longitude
                FROM staging_songs
                WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time
SELECT ts,
        EXTRACT (HOUR FROM ts),
        EXTRACT (DAY FROM ts),
        EXTRACT (WEEK FROM ts),
        EXTRACT (MONTH FROM ts),
        EXTRACT (YEAR FROM ts),
        EXTRACT (DAYOFWEEK FROM ts)
        FROM staging_events
        WHERE ts IS NOT NULL
""")




# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create,songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert,songplay_table_insert]
