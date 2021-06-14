import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession

# read in the config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# setup the aws environment variables
os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """create a sparksession and return it to user"""
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .getOrCreate()
    return spark

def process_song_data(spark, input_data, output_data):
    """parse song data to create songs and artists tables, write to parquet"""
    # get filepath to song data file
    songs_path = input_data + 'song_data'

    # read song data file
    songs_df = spark.read.option("recursiveFileLookup", "true").json(songs_path)
    songs_df.createOrReplaceTempView("songs_json")

    # extract columns to create songs table
    spark.sql("CREATE TABLE songs AS SELECT song_id, title, artist_id, year, duration FROM songs_json;")
    songs_table = spark.sql("""SELECT * FROM songs;""")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_data + "songs/songs_table.parquet")

    # extract columns to create artists table
    spark.sql("""CREATE TABLE artists AS SELECT artist_id, artist_name AS name, artist_location AS location, artist_latitude AS latitude, artist_longitude AS longitude FROM songs_json;""")
    artists_table = spark.sql("""SELECT * FROM artists;""")

    # write artists table to parquet files
    artists_table.write.partitionBy("name").mode("overwrite").parquet(output_data + "artists/artist_table.parquet")


def process_log_data(spark, input_data, output_data):
    """parse log data to create the """
    # get filepath to log data file
    logs_path = input_data + '/log_data/*.json'

    # read log data file
    logs_df = spark.read.json(logs_path)
    logs_df.createOrReplaceTempView("logs_json")

    # extract columns for users table - filtering by actions for song plays
    spark.sql("""CREATE TABLE users AS SELECT userId AS user_id, firstName AS first_name, \
            lastName AS last_name, gender, level FROM logs_json WHERE page = 'NextSong';""")
    users_table = spark.sql("""SELECT * FROM users;""")
    # write users table to parquet files
    users_table.write.partitionBy("user_id").mode("overwrite").parquet(output_data + "users/users_table.parquet") 

    #create multiple udfs for time handling 
    #python datetime expects seconds, not milliseconds
    spark.udf.register("get_timestamp", lambda x: int(x))
    spark.udf.register("get_day", lambda x: datetime.fromtimestamp(x/1000.0).day)
    spark.udf.register("get_hour", lambda x: datetime.fromtimestamp(x/1000.0).hour)
    spark.udf.register("get_week", lambda x: datetime.fromtimestamp(x/1000.0).isocalendar().week)
    spark.udf.register("get_month", lambda x: datetime.fromtimestamp(x/1000.0).month)
    spark.udf.register("get_year", lambda x: datetime.fromtimestamp(x/1000.0).year)
    spark.udf.register("get_weekday", lambda x: datetime.fromtimestamp(x/1000.0).weekday())
    
    # extract columns to create time table - filtering by actions for song plays
    spark.sql("""CREATE TABLE time AS SELECT get_timestamp(ts) AS start_time, get_hour(ts) AS hour, get_day(ts) AS day,\
        get_week(ts) AS week , get_month(ts) AS month, get_year(ts) AS year , \
        get_weekday(ts) AS weekday FROM logs_json WHERE page = 'NextSong';""")
    time_table = spark.sql("""SELECT * FROM time;""")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "times/time_table.parquet")

    # extract columns from joined song and log datasets to create songplays table - filtering by actions for song plays
    spark.sql("""CREATE TABLE songplays AS \
                (SELECT l.ts AS start_time, t.year AS year, t.month AS month,\
                l.userId AS user_id, l.level AS level, \
                s.song_id AS song_id, s.artist_id AS artist_id, \
                l.sessionId AS session_id, \
                s.artist_location AS location, \
                l.userAgent as user_agent \
                FROM time AS t
                JOIN logs_json AS l ON t.start_time = l.ts AND l.page = 'NextSong' \
                JOIN songs_json AS s ON s.artist_name = l.artist);""")                    
    songplays_table = spark.sql("""SELECT * FROM songplays;""")

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "songplays/songplays_table.parquet")

def main():
    """run all above functions to perform the job"""
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://pyspark-music/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
