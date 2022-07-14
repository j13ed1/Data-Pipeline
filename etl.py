# Import Libs

import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (udf, col, year, month, dayofmonth, hour,
    weekofyear, date_format, dayofweek, max, monotonically_increasing_id)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType)
import logging
import boto3
from botocore.exceptions import ClientError
import sys


# Create Spark Session

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']
os.environ['AWS_DEFAULT_REGION']=config['AWS']['AWS_DEFAULT_REGION']


def create_spark_session():
    """Create a Spark session"""
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark




def create_bucket(bucket_name, region=None, acl="private"):
    """
    Create an S3 bucket in a specified region
    
    Args:
        bucket_name: Bucket to create
        region: String region to create bucket in, e.g., 'us-west-2'
        acl: Canned access control list to apply to the bucket. 'public-read'
            makes sure everything posted is publicly readable
    """
    
    # Create bucket
    try:
        if region is None:
            s3_client = boto3.client('s3')
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client = boto3.client('s3', region_name=region)
            location = {'LocationConstraint': region}
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration=location,
                ACL=acl
            )
    except ClientError as e:
        logging.error(e)
        return False
    return True


# Process Song Data
def process_song_data(spark, input_data, output_data):
    """
   
    1. Load data from Song_data to s3
    2. Create songs and artists table
    
    
    Args:
        spark: a Spark session
        input_data: to read song data in from s3
        output_data: write analytics tables to s3
    """
    
    # Load data
    song_data = input_data + "song_data/*/*/*/*.json"
    start_time=time.time()
    df_song_data = spark.read.json(input_song_data)
    
    
     # Songs table data    
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
      # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(
        output_data + "songs_table.parquet",
        mode="overwrite",
        partitionBy=["year", "artist_id"]
    ) 
    
    
        # extract columns to create artists table
    artists_table = (
        df
        .select(
            "artist_id",
            col("artist_name").alias("name"),
            col("artist_location").alias("location"),
            col("artist_latitude").alias("latitude"),
            col("artist_longitude").alias("longitude"))
        .distinct()
    )
    
    # write artists table to parquet files
    artists_table.write.parquet(
        output_data + "artists_table.parquet", mode="overwrite"
    )

    
   # Process Song data 
    
    def process_log_data(spark, input_data, output_data):
    """
    1. Laod dats from Log_data and song_data to S3
    2. create users
    3. write users
    
    Args:
        spark: a Spark session
        input_data: song_data on S3 
        output_data: parquet file S3 
    """
    
    # Raw data
    linput_log_data = input_data + 'log_data/2018/11/*.json'
    input_song_data = input_data + 'song_data/A/B/C/*.json'
    start_time = time.time()
    df_log_data = spark.read.json(input_log_data)
    df_song_data = spark.read.json(input_song_data)

    
        # ----------------------- # 
    #### Setup users table ####
    # ----------------------- #
    # Create table
    df_users_table = df_log_data.where((df_log_data.page == 'NextSong') & (df_log_data.userId.isNotNull()))
    df_users_table = df_users_table.select('userId', 'firstName', 'lastName', 'gender', 'level').dropDuplicates()

    # Rename columns
    column_names = ["user_id", "first_name", "last_name", "gender", "level"]
    df_users_table = df_users_table.toDF(*column_names)

    # Fix Datatypes
    df_users_table = df_users_table.withColumn("user_id",col("user_id").cast('integer'))

    # Write table to s3 --> parquet files partitioned by level
    output_users_data = output_data + 'users/users.parquet'
    df_users_table.write.parquet(output_users_data, 'overwrite')

    # ---------------------- # 
    #### Setup time table ####
    # ---------------------- #
    # Create table
    df_time_table = df_log_data.where((df_log_data.page == 'NextSong') & (df_log_data.ts.isNotNull()))
    df_time_table = df_time_table.select('ts').dropDuplicates()

    # convert unix time to timestamp
    func_ts = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df_time_table = df_time_table.withColumn('start_time', func_ts('ts').cast(TimestampType()))

    # add columns
    df_time_table = df_time_table.withColumn('hour', hour('start_time'))
    df_time_table = df_time_table.withColumn('day', dayofmonth('start_time'))
    df_time_table = df_time_table.withColumn('week', weekofyear('start_time'))
    df_time_table = df_time_table.withColumn('month', month('start_time'))
    df_time_table = df_time_table.withColumn('year', year('start_time'))
    df_time_table = df_time_table.withColumn('weekday', dayofweek('start_time'))

    # remove columns
    df_time_table = df_time_table.drop('ts')

    # Write table to s3 --> parquet files partitioned by year and month
    output_time_data = output_data + 'time/time.parquet'
    df_time_table.write.partitionBy('year', 'month').parquet(output_time_data, 'overwrite')
    
    # --------------------------- # 
    #### Setup songplays table ####
    # --------------------------- #
    # Create table
    df_songplays_table = df_log_data.join(df_song_data, 
                                          (df_log_data.song == df_song_data.title) & (df_log_data.artist == df_song_data.artist_name), 
                                          how='inner')
    # Filter
    df_songplays_table = df_songplays_table.where((df_songplays_table.page == 'NextSong') & (df_songplays_table.ts.isNotNull()))

    # convert unix time to timestamp
    func_ts = udf(lambda ts: datetime.fromtimestamp(ts/1000).isoformat())
    df_songplays_table = df_songplays_table.withColumn('start_time', func_ts('ts').cast(TimestampType()))

    # Add songplay_id
    df_songplays_table = df_songplays_table.withColumn('songplay_id', monotonically_increasing_id())

    # Add columns
    df_songplays_table = df_songplays_table.withColumn('month', month('start_time'))
    df_songplays_table = df_songplays_table.withColumn('year', year('start_time'))

    # Select required columns
    df_songplays_table = df_songplays_table.select('songplay_id', 'start_time', 'userId', 'level', 
                                                   'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', 'month', 'year').dropDuplicates()

    # Rename columns
    column_names = ['songplay_id', 'start_time', 'user_id', 'level', 'song_id', 'artist_id', 'session_id', 'location', 'user_agent', 'month', 'year']
    df_songplays_table = df_songplays_table.toDF(*column_names)

    # Write table to s3 --> parquet files partitioned by year and month
    output_songplays_data = output_data + 'songplays/songplays.parquet'
    df_songplays_table.write.partitionBy('year', 'month').parquet(output_songplays_data, 'overwrite')

##############
#### MAIN ####
##############
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://maitys-sparkify-outputs/"
    
    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)

if __name__ == "__main
    main()
