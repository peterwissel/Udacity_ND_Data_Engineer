class SqlQueries:
    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_events (
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

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs (
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

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS f_songplays (
             songplay_id VARCHAR NOT NULL PRIMARY KEY
            ,user_id INT NOT NULL              --foreign key 
            ,song_id VARCHAR   DISTKEY         --foreign key
            ,artist_id VARCHAR                 --foreign key
            ,session_id INTEGER  
            ,start_time timestamp SORTKEY      --foreign key
            ,level VARCHAR
            ,location VARCHAR
            ,user_agent VARCHAR 
            ,UNIQUE (user_id, start_time)
        );
    """)

    songplay_table_insert = ("""
        INSERT INTO f_songplays (songplay_id, user_id, song_id, artist_id, session_id, start_time, level, location, 
                                 user_agent)
        SELECT DISTINCT md5(events.sessionid || events.start_time) songplay_id,
               events.userid AS user_id,
               songs.song_id AS song_id,
               songs.artist_id AS artist_id,
               events.sessionid AS session_id,
               events.start_time AS start_time,
               events.level AS level,
               events.location AS location,
               events.useragent AS user_agent
        FROM (SELECT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time, *
              FROM staging_events
              WHERE page = 'NextSong'
            ) events
        LEFT JOIN staging_songs songs
               ON events.song = songs.title
              AND events.artist = songs.artist_name
              AND events.length = songs.duration
    
        WHERE user_id NOT IN (
                                SELECT DISTINCT fs.user_id
                                FROM f_songplays fs
                               WHERE fs.user_id = user_id
                                 AND fs.session_id = session_id
                                 AND fs.start_time = start_time
                            )
    ;
    """)

    songplay_table_truncate = ("""
        TRUNCATE TABLE f_songplays;
    """)

    user_table_create = ("""
         CREATE TABLE IF NOT EXISTS d_users (
            user_id INTEGER NOT NULL SORTKEY PRIMARY KEY 
           ,first_name VARCHAR 
           ,last_name VARCHAR 
           ,gender VARCHAR (1)
           ,level VARCHAR 
           )
        DISTSTYLE all ;
    """)

    user_table_insert = ("""
        INSERT INTO  d_users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT
               se.userid    AS user_id
             , se.firstname AS first_name
             , se.lastname  AS last_name
             , se.gender    AS gender
             , se.level     AS level
        FROM staging_events AS se
        JOIN f_songplays AS fs ON fs.user_id = se.userid
       WHERE se.page = 'NextSong'
         AND se.userid NOT IN (
                                SELECT DISTINCT user_id
                                  FROM d_users AS u
                                 WHERE u.user_id = se.userid
                               )
    """)

    user_table_truncate = ("""
        TRUNCATE TABLE d_users;
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS d_songs (song_id VARCHAR NOT NULL SORTKEY DISTKEY PRIMARY KEY 
                                           ,title VARCHAR 
                                           ,artist_id VARCHAR 
                                           ,year INTEGER 
                                           ,duration DECIMAL 
                                       );
    """)

    song_table_insert = ("""
        INSERT INTO  d_songs(song_id, artist_id, title, year, duration)
        SELECT DISTINCT
               ss.song_id AS song_id
             , ss.artist_id AS artist_id
             , ss.title AS title
             , ss.year AS year
             , ss.duration AS duration
          FROM staging_songs AS ss
          JOIN f_songplays fs ON ss.artist_id = fs.artist_id AND ss.song_id = fs.song_id
         WHERE ss.song_id NOT IN (
                                    SELECT DISTINCT ds.song_id
                                      FROM d_songs AS ds
                                     WHERE ds.song_id = ss.song_id
             )
    ;
    """)

    song_table_truncate = ("""
            TRUNCATE TABLE d_songs;
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS d_artists (artist_id VARCHAR NOT NULL SORTKEY PRIMARY KEY 
                                             ,name VARCHAR
                                             ,location VARCHAR  
                                             ,latitude DECIMAL (10,5)  
                                             ,longitude DECIMAL (10,5) 
                                             )
                                             DISTSTYLE all ;
    """)

    artist_table_insert = ("""
        INSERT INTO d_artists(artist_id, name, location, latitude, longitude)
        SELECT DISTINCT 
               ss.artist_id        AS artist_id
             , ss.artist_name      AS name
             , ss.artist_location  AS location
             , ss.artist_latitude  AS latitude
             , ss.artist_longitude AS longitude
        FROM staging_songs AS ss
        JOIN f_songplays fs ON ss.artist_id = fs.artist_id
        WHERE ss.artist_id NOT IN (
                                    SELECT da.artist_id
                                      FROM d_artists AS da
                                     WHERE da.artist_id = ss.artist_id
                                    )
        ;
    """)

    artist_table_truncate = ("""
        TRUNCATE TABLE d_artists;
    """)

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

    time_table_insert = ("""
        INSERT INTO d_time(start_time, hour, day, week, month, year, weekday)
        SELECT    fs.start_time as start_time
                , EXTRACT(HOUR from fs.start_time) AS hour
                , EXTRACT(DAY from fs.start_time) AS day
                , EXTRACT(WEEK from fs.start_time) AS week
                , EXTRACT(MONTH from fs.start_time) AS month
                , EXTRACT(YEAR from fs.start_time) AS year
                , EXTRACT(DOW from fs.start_time) AS weekday
          FROM f_songplays AS fs
          WHERE start_time NOT IN (
                                    SELECT dt.start_time
                                      FROM d_time AS dt
                                     WHERE dt.start_time = fs.start_time
              )
    ;
    """)

    time_table_truncate = ("""
        TRUNCATE TABLE d_time;
    """)

    test_case_has_null_values = ("""
       {%  select count(1) from {{ params.table }}  where {{ params.column }} is null; %}
    """)

    test_case_has_unexpected_content_on_column = ("""
        
    """)

    test_case_has_unvalid_year_range = ("""
    
    """)
