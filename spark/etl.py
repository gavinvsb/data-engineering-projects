import configparser
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    udf, col, monotonically_increasing_id
)
from pyspark.sql.types import TimestampType


# -----------------------------
# Configuration
# -----------------------------
config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["CREDENTIALS"]["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["CREDENTIALS"]["AWS_SECRET_ACCESS_KEY"]


# -----------------------------
# Spark Session
# -----------------------------
def create_spark_session():
    """
    Create or retrieve a Spark session.

    Returns:
        SparkSession: Active Spark session.
    """
    spark = (
        SparkSession.builder
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
        .getOrCreate()
    )
    return spark


# -----------------------------
# Process Song Data
# -----------------------------
def process_song_data(spark, input_data, output_data):
    """
    Process song data from S3 and create songs and artists dimension tables.

    Args:
        spark (SparkSession): Active Spark session.
        input_data (str): Root path to S3 bucket.
        output_data (str): Output directory for parquet files.
    """
    print("--- Starting process_song_data ---")

    # Filepath to song data
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # Read song data
    df = spark.read.json(song_data)

    # Songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration").dropDuplicates(["song_id"])
    songs_table.write.partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs.parquet"), "overwrite")
    print("(1/2) songs.parquet completed")

    # Artists table
    artists_table = df.select(
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ).dropDuplicates(["artist_id"])
    artists_table.write.parquet(os.path.join(output_data, "artists.parquet"), "overwrite")
    print("(2/2) artists.parquet completed")

    print("--- Completed process_song_data ---\n")


# -----------------------------
# Process Log Data
# -----------------------------
def process_log_data(spark, input_data, output_data):
    """
    Process log data from S3 and create users, time, and songplays tables.

    Args:
        spark (SparkSession): Active Spark session.
        input_data (str): Root path to S3 bucket.
        output_data (str): Output directory for parquet files.
    """
    print("--- Starting process_log_data ---")

    # Filepath to log data
    log_data = os.path.join(input_data, "log_data/*/*/*.json")

    # Read log data
    df = spark.read.json(log_data).where(col("page") == "NextSong")

    # Users table (most recent record per user)
    users_table = (
        df.select("userId", "firstName", "lastName", "gender", "level", "ts")
        .orderBy("ts", ascending=False)
        .dropDuplicates(["userId"])
        .drop("ts")
    )
    users_table.write.parquet(os.path.join(output_data, "users.parquet"), "overwrite")
    print("(1/3) users.parquet completed")

    # Time table (convert timestamp)
    get_datetime = udf(lambda x: datetime.fromtimestamp(int(x) / 1000), TimestampType())

    df = df.withColumn("start_time", get_datetime(df.ts))
    time_table = df.select(
        "start_time"
    ).dropDuplicates().withColumn("hour", col("start_time").hour) \
        .withColumn("day", col("start_time").day) \
        .withColumn("week", col("start_time").isocalendar().week) \
        .withColumn("month", col("start_time").month) \
        .withColumn("year", col("start_time").year) \
        .withColumn("weekday", col("start_time").weekday())

    time_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "time.parquet"), "overwrite")
    print("(2/3) time.parquet completed")

    # Songplays table (join logs with songs)
    song_df = spark.read.parquet(os.path.join(output_data, "songs.parquet"))

    songplays_table = (
        df.join(song_df, (song_df.title == df.song) & (song_df.artist_id == df.artist))
        .withColumn("songplay_id", monotonically_increasing_id())
        .select(
            "songplay_id",
            "start_time",
            col("userId").alias("user_id"),
            "level",
            "song_id",
            "artist_id",
            col("sessionId").alias("session_id"),
            "location",
            col("userAgent").alias("user_agent")
        )
    )
    songplays_table.write.partitionBy("year", "month").parquet(os.path.join(output_data, "songplays.parquet"), "overwrite")
    print("(3/3) songplays.parquet completed")

    print("--- Completed process_log_data ---\n")


# -----------------------------
# Main Execution
# -----------------------------
def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "results"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
