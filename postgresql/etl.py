import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Extracts song and artist information from a song JSON file
    and inserts them into the songs and artists tables.
    
    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor of the database connection.
    filepath : str
        Path to the song JSON file.
    """
    # Load song file
    df = pd.read_json(filepath, lines=True)

    # Insert song record
    song_data = df[["song_id", "title", "artist_id", "year", "duration"]].values[0].tolist()
    cur.execute(song_table_insert, song_data)

    # Insert artist record
    artist_data = df[[
        "artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"
    ]].values[0].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Extracts time, user, and songplay information from a log JSON file
    and inserts them into the corresponding tables.
    
    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor of the database connection.
    filepath : str
        Path to the log JSON file.
    """
    # Load log file
    df = pd.read_json(filepath, lines=True)

    # Filter by NextSong actions
    df = df[df["page"] == "NextSong"]

    # Convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # Insert time data records
    time_data = [
        (ts, ts.hour, ts.day, ts.isocalendar()[1], ts.month, ts.year, ts.weekday())
        for ts in t
    ]
    column_labels = ["start_time", "hour", "day", "week", "month", "year", "weekday"]
    time_df = pd.DataFrame(time_data, columns=column_labels)

    for _, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # Load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    # Insert user records
    for _, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # Insert songplay records
    for _, row in df.iterrows():
        # Get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        songid, artistid = results if results else (None, None)

        # Insert songplay record
        songplay_data = (
            pd.to_datetime(row.ts, unit="ms"),
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent,
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterates over all JSON files in a given filepath and processes them 
    using the provided function.
    
    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor of the database connection.
    conn : psycopg2.connection
        Active connection to the database.
    filepath : str
        Directory path containing JSON files.
    func : function
        Function to process each file.
    """
    # Gather all JSON files
    all_files = [
        os.path.abspath(f)
        for root, _, files in os.walk(filepath)
        for f in glob.glob(os.path.join(root, "*.json"))
    ]

    num_files = len(all_files)
    print(f"{num_files} files found in {filepath}")

    # Process each file
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print(f"{i}/{num_files} files processed.")


def main():
    """
    - Establishes connection to the database.
    - Processes song and log data.
    - Closes the database connection.
    """
    conn = psycopg2.connect(
        host="127.0.0.1",
        dbname="sparkifydb",
        user="student",
        password="student"
    )
    cur = conn.cursor()

    process_data(cur, conn, filepath="data/song_data", func=process_song_file)
    process_data(cur, conn, filepath="data/log_data", func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
