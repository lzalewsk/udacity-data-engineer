from datetime import datetime, date
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, DateType
import argparse



def create_spark_session():
    """
    Creates Spark session.
    Gets an existing SparkSession or, if there is no existing one, creates a new one based on the options set in this builder.
    :return: SparkSession
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process the Song dataset of files in JSON format and create the songs and artists
    dimension tables in Spark space (for further use).
    Next tables are writen in parquet format to output_data location.
    
    :param: spark: a sparkSession object
    :param input_data: The URI or local location of input datasets
    :param output_data: the URI of S3 bucket or local location for the output files
    """
    
    # get filepath to song data file
    song_data =  input_data + "/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)
    # df.printSchema()

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()
#     songs_table.createOrReplaceTempView("songs")
    df.createOrReplaceTempView("staging_songs")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data+"songs", 
                              mode='overwrite',
                              partitionBy=["year", "artist_id"]
                             )

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    artists_table = artists_table.withColumnRenamed("artist_name", "name") \
                                 .withColumnRenamed("artist_location", "location") \
                                 .withColumnRenamed("artist_latitude", "latitude") \
                                 .withColumnRenamed("artist_longitude", "longitude") \
                                 .distinct()      
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data+"artists", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process the Log dataset of files in JSON format and create the users, time 
    and songplays dimension tables in Spark space.
    Next tables are writen in parquet format to output_data location.
    
    :param: spark: a sparkSession object
    :param input_data: The URI or local location of input datasets
    :param output_data: the URI of S3 bucket or local location for the output files
    """
    
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    users = df.select(["ts", "userId", "firstName", "lastName", "gender", "level"]) \
                    .withColumnRenamed("userId", "user_id") \
                    .withColumnRenamed("firstName", "first_name") \
                    .withColumnRenamed("lastName", "last_name")
    users.createOrReplaceTempView("users")
    users_table = spark.sql(
            """
                SELECT DISTINCT user_id, first_name, last_name, gender, level
                FROM users
                WHERE user_id is NOT NULL
            """
        )    
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users", mode='overwrite')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000.0), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: date.fromtimestamp(x / 1000.0), DateType())
    df = df.withColumn("date", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select("start_time", 
                           hour("date").alias("hour"), 
                           dayofmonth("date").alias("day"), 
                           weekofyear("date").alias("week"), 
                           month("date").alias("month"),
                           year("date").alias("year"),
                           dayofweek("date").alias("weekday")
                        ).distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time", 
                              mode='overwrite',
                              partitionBy=["year", "month"]
                            )

    # read in song data to use for songplays table
    song_df = df.select("artist",
                        "song",
                        "length", 
                        "page", 
                        "start_time",
                        "userId", 
                        "level", 
                        "sessionId",
                        "location", 
                        "userAgent",
                        month("date").alias("month"),
                        year("date").alias("year"),
                        )

    # extract columns from joined song and log datasets to create songplays table 
    song_df.createOrReplaceTempView("staging_events")

    songplays_table = spark.sql(
            """
            SELECT row_number() OVER (PARTITION BY start_time ORDER BY start_time) as songplay_id,
                   e.start_time, 
                   e.userId AS user_id, 
                   e.level AS level, 
                   s.song_id AS song_id, 
                   s.artist_id AS artist_id, 
                   e.sessionId AS session_id, 
                   e.location AS location, 
                   e.userAgent AS user_agent,
                   e.year,
                   e.month
            FROM staging_events e
            LEFT JOIN staging_songs s 
                   ON e.song = s.title
                  AND e.artist = s.artist_name
                  AND e.length = s.duration
            """
        )
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays", 
                              mode='overwrite',
                              partitionBy=["year", "month"]
                            )

def main():
    """
    Main function of ETL script.
    Depending on selected param options [--emr|--local] diferent 
    input_data and output_data are used.
    """
    
    parser = argparse.ArgumentParser(description='Simple ETL Sparkify datasets processing using Spark')
    parser.add_argument("--emr", help='run Spark processing in AWS EMR Environment',
                        action="store_true")
    parser.add_argument("--local", help='run Spark processing in local environment',
                        action="store_true")
    
    args = parser.parse_args()
 
    if args.emr:
        input_data = "s3a://udacity-dend/"
        output_data = "s3://lzalewsk-emr-bck/sparkify/"
    elif args.local:
        input_data = "data/"
        output_data = "out/"
    else:
        return 0
    
    print(input_data)
    print(output_data)        
    
    spark = create_spark_session()
   
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
