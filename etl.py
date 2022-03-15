import configparser
from datetime import datetime
import os
import glob
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get("AWS",'AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get("AWS",'AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    '''creates and return spark session object'''
     # create spark session
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''process_song_data(): Takes three parameters. 
                            spark: spark session to extract song data from S3, processes that data using Spark, and writes them back to S3.
                            input_data: Contains S3 location to read song data from.
                            output_data: Contains S3 location to write data back to.
                  
            The tasks performed are:
            
                  -gets filepath to song data file
                  -reads song data file
                  -extracts columns to create songs table
                  -writes songs table to parquet files partitioned by year and artist
                  -extracts columns to create artists table
                  -writes artists table to parquet files
     '''
    
    # get filepath to song data file
    song_data = glob.glob(input_data + 'song_data/**/*.json', recursive=True)
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.createOrReplaceTempView("songs_table")
    songs_table = spark.sql (\
                 """select distinct on (song_id) song_id,
                           title, 
                           artist_id,
                           year,
                           duration
                    from songs_table
                 """).toPandas()
    
    
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.to_parquet(path=output_data+'songs.parquet', engine='auto', partition_cols=["year","artist_id"])       

    # extract columns to create artists table
    artists_table = df.createOrReplaceTempView("artists_table")
    artists_table = spark.sql\
                    ("""select distinct on (artist_id) artist_id,
                               artist_name,
                               artist_location,
                               artist_latitude,
                               artist_longitude
                        from artists_table""").toPandas()
    
    # write artists table to parquet files
    artists_table.to_parquet(path=output_data+'artists.parquet', engine='auto')


def process_log_data(spark, input_data, output_data):
    '''process_log_data(): Takes three parameters. 
                           spark: spark session to read log data from S3, processes that data using Spark, and writes them back to S3.
                           input_data: Contains S3 location to read log data from. 
                           output_data: Contains S3 location to write data back to.
                           
            The tasks performed are:
                  
                  -gets filepath to log data file
                  -reads log data file
                  -filters by actions for song plays
                  -extracts columns for users table
                  -writes users table to parquet files
                  -creates timestamp column from original timestamp column
                  -creates datetime column from original timestamp column
                  -extracts columns to create time table
                  -writes time table to parquet files partitioned by year and month
                  -reads in song data to use for songplays table
                  -extracts columns from joined song and log datasets to create songplays table 
                  -writes songplays table to parquet files partitioned by year and month
     '''
    
    # get filepath to log data file
    log_data = glob.glob(input_data + 'log_data/**/*.json', recursive=True)

    # read log data file
    df = spark.read.json(log_data)

    
    # filter by actions for song plays
    df = df[df["page"]=="NextSong"]

    # extract columns for users table    
    df.createOrReplaceTempView("users_table")
    users_table = spark.sql\
                  ("""select distinct on (userId) userId, 
                             firstName, 
                             lastName, 
                             gender, 
                             level
                      from users_table""").toPandas()
    
    # write users table to parquet files
    users_table.to_parquet(path=output_data+'users.parquet', engine='auto')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: int(int(x)/1000.0))
    df = df.withColumn('timestamp', get_timestamp(df['ts']))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime("%Y-%m-%d %H:%M:%S"))
    df = df.withColumn('start_time', get_datetime(df['ts']))
    
    # extract columns to create time table
    get_hour = udf(lambda x: datetime.fromtimestamp(x/1000.0).hour)
    df = df.withColumn('hour', get_hour(df['ts']))

    get_day = udf(lambda x: datetime.fromtimestamp(x/1000.0).day)
    df = df.withColumn('day', get_day(df['ts']))

    get_week = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%W'))
    df = df.withColumn('week', get_week(df['ts']))

    get_month = udf(lambda x: datetime.fromtimestamp(x/1000.0).month)
    df = df.withColumn('month', get_month(df['ts']))

    get_year = udf(lambda x: datetime.fromtimestamp(x/1000.0).year)
    df = df.withColumn('year', get_year(df['ts']))

    get_weekday = udf(lambda x: datetime.fromtimestamp(x/1000.0).strftime('%A'))
    df = df.withColumn('weekday', get_weekday(df['ts']))

    df.createOrReplaceTempView('time_table')
    time_table = spark.sql\
                 ("""select distinct on (start_time) start_time, 
                            hour,
                            day,
                            week,
                            month,
                            year,
                            weekday
                     from time_table""").toPandas()
    
    
    # write time table to parquet files partitioned by year and month
    time_table.to_parquet(path=output_data+'time.parquet', engine='auto', partition_cols=["year","month"])

    # read in song data to use for songplays table
    song_data = glob.glob(input_data + 'song_data/**/*.json', recursive=True)
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.createOrReplaceTempView('songs_table')
    df = df.withColumn('songplay_id', F.monotonically_increasing_id())
    df.createOrReplaceTempView('songplays_table')

    songplays_table = spark.sql\
                  ("""select sp.songplay_id,
                             sp.start_time,
                             sp.userId as user_id,
                             sp.level,
                             s.song_id,
                             s.artist_id,
                             sp.sessionId as session_id,
                             sp.location,
                             sp.userAgent as user_agent,
                             sp.year, 
                             sp.month
                    from songplays_table sp
                    join songs_table s
                    on sp.artist=s.artist_name and sp.song=s.title and sp.length=s.duration""").toPandas()


    # write songplays table to parquet files partitioned by year and month
    songplays_table.to_parquet(path=output_data+'songplays.parquet', engine='auto', partition_cols=["year","month"])


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = config.get("AWS","AWS_BUCKET")
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
