# Sparkify ETL

ETL Demo project

---

## Description
In this project, I try to implement data model and ETL pipeline for parsing JSON log files from activity of Sparkify App..
Data model was implemented with Postgres and build an ETL pipeline using Python.  


## Usage

To check project please run following steps:  

1. Database and table creation
```bash
# python create_tables.py
```

2. Run
```bash
# python etl.py
```  


3. Result of JSON log file ETL process could be check using `test.ipynb` or by CLI `psql -h localhost sparkifydb studen` directly in DB

Optionally in `etl.py` there was added `bulk_process_song_data()` function. To check how it works, please uncomment it in `main()`:

``` python
def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    bulk_process_song_data(conn, filepath='data/song_data') # example of bulk insert
#     process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()
```

## TODO
1. timestamp format in DB int or timestamp? -> timestamp [done]
2. index creation for performance -> example indexes were created [done]
3. only 69 artists id DB and more in logs ... [verified]
4. ...



Comments regarding to review feedback
=====================================
1. **PRIMARY KEY** sugestion in *songplays*  

    ```python 
    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        timestamp TIMESTAMP PRIMARY KEY, 
    #[...]
        );
    """)
    ```  
    
    I think that there could be the situation when two events in log files will have the same `timestamp` i.e. when logs will come from many app servers.
    I have decided to use FOREIGN KEY instead.  
    
    ```python 
    songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        timestamp TIMESTAMP REFERENCES time(timestamp), 
    #[...]
        );
    """)
    ```    

2. **FOREIGN KEYs** - thenks, of course you are rught.
Because of FOREIGN KEY's there was a need to change table creation order in *sql_queries.py*:  
```python 
# create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
# drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
drop_table_queries = [user_table_drop, artist_table_drop, song_table_drop, time_table_drop, songplay_table_drop]
```

Additionaly in `process_song_file` function in etl.py I have changed order of tables filling up. First `artist_table` then `song_data`.  


2. **INDEXes**  
Becasue I mention in TODOs about indexes for increasing performance of any further queries, as example I have added indexes for `songplays` table and for `songs.title` and `artists.artist_name` using gin INDEX.  
Therefore in user_table_create SQL I put:  

``` sql
CREATE EXTENSION pg_trgm;
```  


## License
[MIT](https://choosealicense.com/licenses/mit/) and Udacity Sudents license :).

## Project status
Under developing. Changes after feedback.