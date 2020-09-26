import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# CREATE SCHEMA --> Project3 = Data Warehouse on AWS
create_schema_for_projct3 = "CREATE SCHEMA IF NOT EXISTS project3;"
set_search_path_to_project3 = "SET search_path TO project3;"

# DROP SCHEMA
drop_schema_for_project3 = "DROP SCHEMA IF EXISTS project3 CASCADE;"

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS s_staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS s_staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS f_songplays;"
user_table_drop = "DROP TABLE IF EXISTS d_users;"
song_table_drop = "DROP TABLE IF EXISTS d_songs;"
artist_table_drop = "DROP TABLE IF EXISTS d_artists;"
time_table_drop = "DROP TABLE IF EXISTS d_time;"

# CREATING TABLES

# STAGING TABLE s_staging_events
staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS s_staging_events (
      artist        VARCHAR 
    , auth          VARCHAR 
    , firstName     VARCHAR 
    , gender        VARCHAR 
    , itemInSession INTEGER 
    , lastName      VARCHAR
    , length        DECIMAL 
    , level         VARCHAR 
    , location      VARCHAR 
    , method        VARCHAR 
    , page          VARCHAR 
    , registration  DECIMAL 
    , sessionId     INTEGER 
    , song          VARCHAR 
    , status        INTEGER 
    , ts            BIGINT 
    , userAgent     VARCHAR 
    , userId        INTEGER 
);
""")

# STAGING TABLE s_staging_songs
staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS s_staging_songs (
      num_songs         INTEGER 
    , artist_id         VARCHAR 
    , artist_latitude   DECIMAL 
    , artist_longitude  DECIMAL 
    , artist_location   VARCHAR 
    , artist_name       VARCHAR 
    , song_id           VARCHAR 
    , title             VARCHAR 
    , duration          DECIMAL 
    , year              INTEGER                                      
);
""")


# FACT TABLE

# CREATING TABLE f_songplays
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS f_songplays (songplay_id bigint IDENTITY(0, 1) NOT NULL PRIMARY KEY
                                       ,user_id INT NOT NULL              --foreign key 
                                       ,song_id VARCHAR   DISTKEY         --foreign key
                                       ,artist_id VARCHAR                 --foreign key
                                       ,start_time timestamp SORTKEY      --foreign key
                                       ,session_id INTEGER  
                                       ,level VARCHAR
                                       ,location VARCHAR
                                       ,user_agent VARCHAR 
                                       ,UNIQUE (user_id, start_time)
                                       );
""")

# CREATING DIMENSION TABLES

# CREATING TABLE d_users
user_table_create = ("""
CREATE TABLE IF NOT EXISTS d_users (user_id INTEGER NOT NULL SORTKEY PRIMARY KEY 
                                   ,first_name VARCHAR 
                                   ,last_name VARCHAR 
                                   ,gender VARCHAR (1)
                                   ,level VARCHAR 
                                   )
                                   DISTSTYLE all ;
""")

# CREATING TABLE d_songs
song_table_create = ("""
CREATE TABLE IF NOT EXISTS d_songs (song_id VARCHAR NOT NULL SORTKEY DISTKEY PRIMARY KEY 
                                   ,title VARCHAR 
                                   ,artist_id VARCHAR 
                                   ,year INTEGER 
                                   ,duration DECIMAL 
                                   );
""")

# CREATING TABLE d_artists
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS d_artists (artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY 
                                     ,name VARCHAR
                                     ,location VARCHAR  
                                     ,latitude DECIMAL (10,5)  
                                     ,longitude DECIMAL (10,5) 
                                     )
                                     DISTSTYLE all ;
""")

# CREATING TABLE d_time
time_table_create = ("""
CREATE TABLE IF NOT EXISTS d_time (start_time TIMESTAMP NOT NULL SORTKEY PRIMARY KEY 
                                  ,hour  INTEGER NOT NULL 
                                  ,day INTEGER NOT NULL 
                                  ,week INTEGER NOT NULL
                                  ,month INTEGER NOT NULL
                                  ,year INTEGER NOT NULL
                                  ,weekday VARCHAR NOT NULL
                                  )
                                  DISTSTYLE all; 
""")

# INSERT DATA INTO ... TABLES #

# INSERT DATA INTO STAGING TABLE s_staging_events
staging_events_copy = ("""
    COPY s_staging_events 
    FROM {} 
    IAM_ROLE {}
    JSON {}
    REGION {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'],
            config['S3']['REGION_NAME'])


# INSERT DATA INTO STAGING TABLE s_staging_songs
staging_songs_copy = ("""
    COPY s_staging_songs 
    FROM {}
    IAM_ROLE {}
    JSON 'auto'
    REGION {};
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['REGION_NAME'])


# DIMENSION TABLES

# INSERT DATA INTO FACT TABLE f_sonplays
songplay_table_insert = ("""
insert into f_songplays(user_id, song_id, artist_id, start_time, session_id, level, location, user_agent)
    select distinct
              se.userid as user_id
            , ss.song_id as song_id
            , ss.artist_id as artist_id
            , TIMESTAMP 'epoch' + se.ts/1000 *INTERVAL '1 second' as start_time
            , se.sessionid as session_id
            , se.level as level
            , se.location as location
            , se.useragent as user_agent
      from s_staging_events se
      left join s_staging_songs ss on trim(se.artist) = trim(ss.artist_name) and trim(se.song) = trim(ss.title)
     where 1=1
       and se.page = 'NextSong'
       and user_id NOT IN (
                            select distinct fs.user_id
                            from f_songplays fs
                           where fs.user_id = user_id
                             and fs.session_id = session_id
                             and fs.start_time = start_time
         )
;
""")

# INSERT DATA INTO DIMENSION TABLE d_users
user_table_insert = ("""
insert  into d_users (user_id, first_name, last_name, gender, level)
    select distinct
           se.userid    as user_id
         , se.firstname as first_name
         , se.lastname  as last_name
         , se.gender    as gender
         , se.level     as level
    from s_staging_events as se
    join f_songplays as fs on fs.user_id = se.userid
    where se.page = 'NextSong'
      and se.userid not in (
                            select distinct user_id
                              from d_users as u
                             where u.user_id = se.userid
                           )
;
""")

# INSERT DATA INTO DIMENSION TABLE d_songs
song_table_insert = ("""
insert into d_songs(song_id, artist_id, title, year, duration)
    select distinct
           ss.song_id as song_id
         , ss.artist_id as artist_id
         , ss.title as title
         , ss.year as year
         , ss.duration as duration
      from s_staging_songs as ss
      join f_songplays fs on ss.artist_id = fs.artist_id and ss.song_id = fs.song_id
     where ss.song_id not in (
                                select distinct ds.song_id
                                  from d_songs as ds
                                 where ds.song_id = ss.song_id
         )
;
""")

# INSERT DATA INTO DIMENSION TABLE d_artists
artist_table_insert = ("""
insert into d_artists(artist_id, name, location, latitude, longitude)
    select distinct
           ss.artist_id        as artist_id
         , ss.artist_name      as name
         , ss.artist_location  as location
         , ss.artist_latitude  as latitude
         , ss.artist_longitude as longitude
    from s_staging_songs as ss
    join f_songplays fs on ss.artist_id = fs.artist_id
    where ss.artist_id not in (select da.artist_id
                                from d_artists as da
                               where da.artist_id = ss.artist_id
       )
;
""")

# INSERT DATA INTO DIMENSION TABLE d_time
time_table_insert = ("""
insert into d_time(start_time, hour, day, week, month, year, weekday)
    select    fs.start_time as start_time
            , EXTRACT(HOUR from fs.start_time) as hour
            , EXTRACT(DAY from fs.start_time) as day
            , EXTRACT(WEEK from fs.start_time) as week
            , EXTRACT(MONTH from fs.start_time) as month
            , EXTRACT(YEAR from fs.start_time) as year
            , EXTRACT(DOW from fs.start_time) as weekday
      from f_songplays as fs
      where start_time not in (
                                select dt.start_time
                                  from d_time as dt
                                 where dt.start_time = fs.start_time
          )
    ;
""")

# SCHEMA COMMANDS
drop_schema_for_project3_query = [drop_schema_for_project3]
create_schema_and_search_path = [create_schema_for_projct3, set_search_path_to_project3]


# QUERY LISTS
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop, time_table_drop]


create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,
                        user_table_create, song_table_create, artist_table_create, time_table_create]


copy_table_queries = [staging_events_copy, staging_songs_copy]

insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert,
                        time_table_insert]

clean_up_after_processing = [staging_events_table_drop, staging_songs_table_drop]
