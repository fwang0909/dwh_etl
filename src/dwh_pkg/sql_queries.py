import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('../../dwh.cfg')
role=config.get("IAM_ROLE", "ARN")
print (role)
log_data=config.get("S3", "LOG_DATA")
song_data=config.get("S3","SONG_DATA")

#schma
create_schema_str= "CREATE SCHEMA IF NOT EXISTS dev;"
set_search_path="SET search_path TO dev;"

# DROP TABLES

staging_events_table_drop = "drop table IF EXISTS staging_events;"
staging_songs_table_drop = "drop table IF EXISTS staging_songs;"
songplay_table_drop = "drop table IF EXISTS songplays;"
user_table_drop = "drop table IF EXISTS users;"
song_table_drop = "drop table IF EXISTS songs;"
artist_table_drop = "drop table IF EXISTS artists;"
time_table_drop = "drop table IF EXISTS time;"

# CREATE TABLES


staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events 
     ( 
          num_events    INTEGER identity(0,1), 
          artist        VARCHAR, 
          auth          VARCHAR, 
          firstName     VARCHAR, 
          gender        VARCHAR, 
          itemInSession INTEGER, 
          lastName      VARCHAR, 
          length        VARCHAR, 
          level         VARCHAR, 
          location      VARCHAR, 
          method        VARCHAR, 
          page          VARCHAR, 
          registration  VARCHAR, 
          sessionId     INTEGER, 
          song          VARCHAR, 
          status        VARCHAR, 
          ts            VARCHAR, 
          userAgent     VARCHAR, 
          userId INTEGER);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs 
 ( 
      artist_id        VARCHAR, 
      artist_latitude  VARCHAR, 
      artist_location  VARCHAR, 
      artist_longitude VARCHAR, 
      artist_name      VARCHAR, 
      duration DOUBLE PRECISION, 
      num_songs INTEGER, 
      song_id   VARCHAR, 
      title     VARCHAR, 
      year      INTEGER 
 );
""")

songplay_table_create = (""" 
CREATE TABLE songplays 
  ( 
     songplay_id BIGINT  identity(0,1) PRIMARY KEY NOT NULL, 
     start_time  TIMESTAMP NOT NULL sortkey, 
     user_id     INTEGER NOT NULL, 
     level       VARCHAR , 
     song_id     VARCHAR distkey, 
     artist_id   VARCHAR , 
     session_id  INTEGER , 
     location    VARCHAR, 
     user_agent  VARCHAR
  ); 
""")

user_table_create = ("""
CREATE TABLE users 
  ( 
     user_id    INTEGER PRIMARY KEY NOT NULL sortkey, 
     first_name VARCHAR, 
     last_name  VARCHAR, 
     gender     VARCHAR, 
     level      VARCHAR 
  ); 
""")

song_table_create = (""" 
CREATE TABLE IF NOT EXISTS songs 
  ( 
     song_id   VARCHAR PRIMARY KEY NOT NULL sortkey distkey, 
     title     VARCHAR, 
     artist_id VARCHAR, 
     year      INTEGER, 
     duration  DOUBLE PRECISION 
  ); 
""")

artist_table_create = ("""
CREATE TABLE artists 
  ( 
     artist_id  VARCHAR PRIMARY KEY NOT NULL sortkey, 
     NAME       VARCHAR, 
     location   VARCHAR, 
     latiude    VARCHAR, 
     longtitude VARCHAR 
  ); 
""")

time_table_create = ("""
CREATE TABLE time 
(
    start_time TIMESTAMP PRIMARY KEY NOT NULL sortkey, 
     hour       INTEGER, 
     day        INTEGER, 
     week       INTEGER, 
     month      INTEGER, 
     year       INTEGER, 
     weekday    INTEGER 
)
""")



# STAGING TABLES
staging_events_copy = ("""
COPY staging_events from {}
credentials 'aws_iam_role={}'
format as json 'auto ignorecase';
""".format(log_data, role))


staging_songs_copy = ("""
COPY staging_songs from {}
credentials 'aws_iam_role={}'
format as json 'auto ignorecase';
""".format(song_data, role))

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays  (
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location,
    user_agent  
)SELECT
    distinct TIMESTAMP 'epoch' + (events.ts)/1000 *INTERVAL '1second' as t_start_time , events.userid, events.level, songs.song_id, songs.artist_id,  events.sessionid, events.location, events.useragent
FROM
    staging_songs AS songs 
JOIN
    staging_events AS events ON songs.title = events.song  
    AND songs.artist_name = events.artist 
WHERE
    events.song IS NOT NULL  
    AND events.artist IS NOT NULL 
    and events.page = 'NextSong' 
    and events.userid is NOT NULL;
""")

user_table_insert = ("""
INSERT INTO users 
            ( 
                        user_id, 
                        first_name, 
                        last_name, 
                        gender, 
                        level 
            ) 
select  events.userid,events.firstName, events.lastName, events.gender, events.level
from staging_events as events where events.userid is NOT NULL ;
""")

song_table_insert = ("""
INSERT INTO songs  (
    song_id,
    title,
    artist_id,
    year,
    duration  
) select
    staging_songs.song_id, staging_songs.title, staging_songs.artist_id , staging_songs.year, staging_songs.duration 
from
    staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists 
            ( 
                        artist_id , 
                        name, 
                        location, 
                        latiude, 
                        longtitude 
            ) 
SELECT staging_songs.artist_id, 
       staging_songs.artist_name, 
       staging_songs.artist_location, 
       staging_songs.artist_latitude, 
       staging_songs.artist_longitude 
FROM   staging_songs ;
""")


time_table_insert = ("""
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
SELECT a.start_time as start_time, 
       Extract (hour FROM a.start_time)    AS hour, 
       Extract (day FROM a.start_time)     AS day, 
       Extract (week FROM a.start_time)    AS week , 
       Extract (month FROM a.start_time)   AS month, 
       Extract (year FROM a.start_time)    AS year, 
       Extract (weekday FROM a.start_time) AS weekday 
FROM   ( 
                       SELECT DISTINCT timestamp 'epoch' + (events.ts)/1000 *INTERVAL '1second' AS start_time
                       FROM            staging_events                                           AS events) a ;
""")



# QUERY LISTS
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
