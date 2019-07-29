import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
#Routine table drop to prevent descripant data
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES
"""Star Schema Setup with staging tables for data inserts.
  For dimension tables -select sort keys based on
  column most likley to be searched
  and most likley to be joined"""
staging_events_table_create= ("""CREATE TABLE staging_events
(artist           VARCHAR(500),
    auth             VARCHAR(25),
    firstName        VARCHAR(51),
    gender           VARCHAR(2),
    itemInSession    INTEGER,
    lastName         VARCHAR(50),
    length           REAL,
    level            VARCHAR(5),
    location         VARCHAR(75),
    method           VARCHAR(10),
    page             VARCHAR(25),
    registration     FLOAT,
    sessionId        INTEGER,
    song             VARCHAR(300),
    status           INTEGER,
    ts               BIGINT,
    userAgent        VARCHAR(350),
    userId           INTEGER
);"""
)

staging_songs_table_create = ("""CREATE TABLE staging_songs
(artist_id        VARCHAR(50),
    artist_latitude  REAL,
    artist_longitude  REAL,
    artist_location  VARCHAR(300),
    artist_name      VARCHAR(100),
    duration         REAL,
    num_song         INTEGER,
    song_id          VARCHAR(50),
    title            VARCHAR(300),
    year             INTEGER

);"""
)

songplay_table_create = (""" CREATE TABLE songplay
(
    songplay_id    INT IDENTITY(1, 1),
    start_time     BIGINT NOT NULL sortkey,
    user_id        INTEGER NOT NULL distkey,
    level          VARCHAR(5),
    song_id        VARCHAR(50) NOT NULL,
    artist_id      VARCHAR(35) NOT NULL,
    session_id     INTEGER NOT NULL,
    location       VARCHAR(50) NOT NULL,
    user_agent     VARCHAR(350) NOT NULL
);"""
)

user_table_create = (""" CREATE TABLE users
(
    user_id      INTEGER NOT NULL sortkey,
    first_name   VARCHAR(25) NOT NULL,
    last_name    VARCHAR(25) NOT NULL,
    gender       VARCHAR(1) NOT NULL,
    level        VARCHAR(5) NOT NULL
)diststyle all;"""
)

song_table_create = (""" CREATE TABLE songs
(
    song_id      VARCHAR(50) NOT NULL ,
    title        VARCHAR(300) NOT NULL,
    artist_id    VARCHAR(35) NOT NULL,
    year         INTEGER  NOT NULL,
    duration     INTEGER NOT NULL
)COMPOUND SORTKEY(song_id, artist_id);"""
)

artist_table_create = (""" CREATE TABLE artists
(
    artist_id    VARCHAR(50) NOT NULL sortkey,
    name         VARCHAR(500),
    location     VARCHAR(300) ,
    lattitude    REAL,
    longitude    REAL
)diststyle all;"""
)

time_table_create = (""" CREATE TABLE time
(
    time_key     BIGINT NOT NULL sortkey,
    start_time   Date NOT NULL,
    hour         INTEGER NOT NULL,
    day          INTEGER NOT NULL,
    week         INTEGER NOT NULL,
    month        INTEGER NOT NULL,
    year         INTEGER NOT NULL,
    weekday      INTEGER NOT NULL
)diststyle all;"""
)

# STAGING TABLES
#Copy of data into staging tables
DWH_ROLE_ARN = config.get("DWH", "DWH_ROLE_ARN")

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data' TRUNCATECOLUMNS
    credentials 'aws_iam_role={}'
    json 's3://udacity-dend/log_json_path.json' region 'us-west-2'
""").format(DWH_ROLE_ARN)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data' TRUNCATECOLUMNS
    credentials 'aws_iam_role={}'
    json 'auto' region 'us-west-2'
""").format(DWH_ROLE_ARN)


# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplay( start_time, user_id, level, song_id, artist_id, \
session_id, location, user_agent)
SELECT
ts AS start_time,
userId AS user_id,
level AS level,
song_id AS song_id,
artist_id AS artist_id,
sessionid AS Session_id,
location AS location,
userAgent AS user_agent
from staging_events se, staging_songs ss
where se.artist = ss.artist_name
and se.song = ss.title
and page = 'NextSong';
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT
userId AS user_id,
firstName AS first_name,
lastName AS last_name,
gender AS gender,
level AS level
from staging_events se
where page = 'NextSong'
and userid not in
(Select distinct(user_Id) from users u
where u.user_id = se.userid)
group by userid, firstname, lastname, gender, level;
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT
song_id AS song_id,
title AS title,
artist_id AS artist_id,
year AS year,
duration AS duration
from staging_songs;
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, lattitude, longitude)
SELECT
artist_id AS artist_id,
artist_name AS name,
artist_location AS location,
artist_latitude AS lattitude,
artist_longitude AS longitude
from staging_songs ss
where artist_id not in (
Select distinct (artist_id) from artists a
where a.artist_id = ss.artist_id)
group by artist_id, artist_name, artist_location, artist_latitude, artist_longitude;
""")

time_table_insert = ("""INSERT INTO time(time_key,start_time, hour, day, week, month, year, weekday)
SELECT
ts as time_key,
(TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')as start_time,
extract (h from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as hour,
extract (d from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as day,
extract (w from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as Week,
extract (mon from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as month,
extract (year from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as year,
extract (dow from (TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second')) as weekday
from staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
drop_list = ['staging_events_table_drop', 'staging_songs_table_drop', 'songplay_table_drop', 'user_table_drop', 'song_table_drop', 'artist_table_drop', 'time_table_drop']
create_list = ['staging_events_table_create', 'staging_songs_table_create', 'songplay_table_create', 'user_table_create', 'song_table_create', 'artist_table_create', 'time_table_create']
copy_table = ['staging_events_copy','staging_songs_copy']
insert_table = ['songplay_table_insert', 'user_table_insert', 'song_table_insert', 'artist_table_insert', 'time_table_insert']
