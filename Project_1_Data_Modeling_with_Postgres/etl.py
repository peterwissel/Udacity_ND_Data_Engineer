import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process song file data
    - get all available files based on the given filepath
    - prepare song_data to insert it into dimension table songs 
    Information: (d_songs --> I marked all dimension tables with prefix "d_" to distinguish between fact (f_) and 
    dimension tables)
    - prepare artist data and insert it into d_artist dimension table
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    # df = pd.read_json('data/log_data/2018/11/2018-11-01-events.json', lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[
        0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process log_file data 
    - read files by given filepath
    - filter only entries with value "NextSong" from column "page"
    - convert the numeric based timestamp (ts) into datatype timestamp 
    - prepare time data combined with column names and put them together into a DataFrame
    - insert time information into table d_time
    - insert user data into dimension d_users
    - prepare songplay records for insert into fact table f_songplays with foreign key informaiton 
      from dimension tables d_songs and d_artists
    - 
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime 
    t = pd.to_datetime(df['ts'], unit='ms')

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.weekofyear, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['start_time', 'hour', 'day', 'week_of_year', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)), orient='columns', dtype=None)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))

        # fetch all item out of cursor object into results object
        results = cur.fetchone()

        # if results is true --> put result values into variables
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [row.userId, songid, artistid, pd.to_datetime(row.ts, unit='ms'), row.sessionId, row.level,
                         row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    - parse thru the file system for files based on the given path from variable "filepath"
    - print out total number of found files
    - iterate over all found files and print out information about it
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Build ETL Pipeline for Sparkify song play data:
    - create connection to postgres db via psycopg2 library
    - instanciate cursor object
    - process song file data and log file data
    
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
