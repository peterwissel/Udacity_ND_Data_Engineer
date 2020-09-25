# DROP TABLES
from typing import List

songplay_table_drop = "DROP TABLE IF EXISTS f_songplays;"
user_table_drop = "DROP TABLE IF EXISTS d_users;"
song_table_drop = "DROP TABLE IF EXISTS d_songs;"
artist_table_drop = "DROP TABLE IF EXISTS d_artists;"
time_table_drop = "DROP TABLE IF EXISTS d_artists;"

# CREATE TABLES
"""
- Create tables area for creating:
-- fact tables: f_songplays
-- dimension tables: d_users, d_songs, d_artists, d_time
"""

# create fact tables
songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS f_songplays (songplay_id serial NOT NULL PRIMARY KEY 
                                       ,user_id int NOT NULL           --foreign key 
                                       ,song_id text                   --foreign key
                                       ,artist_id text                 --foreign key
                                       ,start_time timestamp           --foreign key
                                       ,session_id int 
                                       ,level text
                                       ,location text
                                       ,user_agent text 
                                       ,UNIQUE (user_id, start_time)
                                       );
""")

# create dimension tables
user_table_create = ("""
CREATE TABLE IF NOT EXISTS d_users (user_id int NOT NULL PRIMARY KEY 
                                   ,first_name text
                                   ,last_name text
                                   ,gender char(1)
                                   ,level text
                                   );
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS d_songs (song_id text NOT NULL PRIMARY KEY 
                                   ,title text
                                   ,artist_id text 
                                   ,year int
                                   ,duration numeric 
                                   );
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS d_artists (artist_id text NOT NULL PRIMARY KEY 
                                     ,name text
                                     ,location text  
                                     ,latitude numeric(10,5)  
                                     ,longitude numeric(10,5) 
                                     );
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS d_time (start_time timestamp NOT NULL PRIMARY KEY 
                                  ,hour  int NOT NULL 
                                  ,day int NOT NULL 
                                  ,week int NOT NULL
                                  ,month int NOT NULL
                                  ,year int NOT NULL
                                  ,weekday text NOT NULL
                                  ) 
""")

# INSERT RECORDS
"""
- Insert statements to fill up the tables
"""
songplay_table_insert = ("""
INSERT INTO f_songplays (user_id, song_id, artist_id, start_time, session_id, level, location, user_agent)
            values (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (songplay_id) 
            DO NOTHING;
""")

user_table_insert = ("""
INSERT INTO d_users (user_id, first_name, last_name, gender, level)
             values (%s, %s, %s, %s, %s)
             ON CONFLICT (user_id)
             DO UPDATE SET 
               (first_name, last_name, gender, level)=
               (EXCLUDED.first_name, EXCLUDED.last_name, EXCLUDED.gender, EXCLUDED.level)
               ;
""")

song_table_insert = ("""
INSERT INTO d_songs(song_id, title, artist_id, year, duration)
            values (%s, %s, %s, %s, %s)
            ON CONFLICT (song_id)
            DO NOTHING;
""")


artist_table_insert = ("""
INSERT INTO d_artists (artist_id, name, location, latitude, longitude)
               values (%s, %s, %s, %s, %s)
               ON CONFLICT (artist_id)
               DO NOTHING
               ;
""")

time_table_insert = ("""
INSERT INTO d_time (start_time, hour, day, week, month, year, weekday) 
            values (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (start_time) 
            DO NOTHING
            ;
""")

# FIND SONGS
"""
- get song_id and artist_id to update the fact table with correct foreign-keys for the dimensions songs and artists
"""
song_select = ("""
select s.song_id, a.artist_id
  from d_songs s
  join d_artists a on s.artist_id = a.artist_id
 where s.title = %s
   and a.name = %s
   and s.duration = %s
;
""")

# QUERY LISTS
""" 
- list of queries to execute 
"""
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

