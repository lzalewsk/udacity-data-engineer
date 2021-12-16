import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

from io import StringIO
import csv

def bulk_insert(conn, df, table, use_index=True, index_label='id'):
    """
    Bulk insert of DataFrame to DB using in memory
    buffer and copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, 
              index_label=index_label, 
              header=False,
              index=use_index,
              quoting=csv.QUOTE_NONE,
              sep='|'
             )
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep="|",null="")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
#         os.remove(tmp_df)
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()

def process_song_file(cur, filepath):
    # open song file
    df = pd.read_json(filepath,lines=True)

    # insert artist record
    artist_data = df[['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude'
                     ]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = df[['song_id',
                    'title',
                    'artist_id',
                    'year',
                    'duration'
                   ]].values[0].tolist()
    
    cur.execute(song_table_insert, song_data)
    

def process_log_file(cur, filepath):
    # open log file
    df = pd.read_json(filepath,lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df.ts,unit='ms')
    
    # insert time data records
    time_data = (
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.weekofyear,
        t.dt.month,
        t.dt.year,
        t.dt.weekday
    )
    
    column_labels = [
        'timestamp',
        'hour',
        'day',
        'weekofyear',
        'month',
        'year',
        'weekday'
    ]
    
    time_df = pd.DataFrame(dict(zip(column_labels,[x.tolist() for x in time_data])))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[[
        'userId',
        'firstName',
        'lastName',
        'gender',
        'level'
    ]].copy()

    # cleaning data
    # of course it will be also done during insering (on DB siede)
    user_df.drop_duplicates(inplace=True)
    # skiping records with no firstName 
    user_df = user_df[~user_df.firstName.isna()]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    
    # unification of timestamp
    df.ts = pd.to_datetime(df.ts,unit='ms')
    # cleaning missing userId data
    df.userId = df.userId.replace('',-1)
    
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
#             print(row.song, row.artist, row.length)
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data =  (
            row.ts, 
            row.userId,
            row.level, 
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))

        
def bulk_process_song_data(conn, filepath):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # parse all files to one DataFrame
    dfSongData = pd.DataFrame()
    for filepath in all_files:
        df = pd.read_json(filepath,lines=True)
        dfSongData = dfSongData.append(df,ignore_index=True)
    
    # get artist record
    df_artist_data = dfSongData[['artist_id',
                              'artist_name',
                              'artist_location',
                              'artist_latitude',
                              'artist_longitude'
                             ]].drop_duplicates().copy()
    #export to DB
    bulk_insert(conn,df_artist_data,'artists',use_index=False)    
    
    # get song_data to insert
    df_song_data = dfSongData[['song_id',
                            'title',
                            'artist_id',
                            'year',
                            'duration'
                           ]].drop_duplicates().copy()

    #export to DB
    bulk_insert(conn,df_song_data,'songs',use_index=False)    
        
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

#     bulk_process_song_data(conn, filepath='data/song_data') # example of bulk insert
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()