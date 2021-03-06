# DROP TABLES
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES
# ?? when inserting data will redferences matter (think of order and constraint)
# also think of timestamptz tiem when ingesting and convert at source (json/pandas)
# note: No NULL value can not be accepted in PRIMARY KEY https://tinyurl.com/veyy8hm
# Note on NUMERIC:
# NUMERIC (6,4) has values with 6 digits, 4 of which are after the decimal point, such as 12.3456
'''
user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id INT GENERATED BY DEFAULT AS IDENTITY UNIQUE PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender VARCHAR(1),
        level VARCHAR(10)
    );
    """
    )
'''

# Note on user table. Duplicates user_id entries exist as level can be free or paid with same user_id
# Some strategies can be put in place to solve this, depends on business case too.
#       - User can't have free and paid memebership at same time
#         Solution: remove free duplicates if same exist with paid
#       - Users can have both free and paid membership (or at least entry must/can exist in DB, for
#         examlple to retain the data in case of a paid cancellation / return to free)
#         Solution 1: create a default primary key independent of user_id.
#         Solution 2: create a composite key (user_id, level)
user_table_create = (
    """
    CREATE TABLE IF NOT EXISTS users (
        uid INT GENERATED BY DEFAULT AS IDENTITY UNIQUE PRIMARY KEY,
        user_id INT,
        first_name TEXT,
        last_name TEXT,
        gender VARCHAR(1),
        level VARCHAR(10)
    );
    """
    )


artist_table_create = (
    """
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT PRIMARY KEY,
        artist_name TEXT,
        artist_location TEXT,
        artist_latitude NUMERIC(6, 3),
        artist_longitude NUMERIC(6, 3)
    );
    """
    )


song_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT PRIMARY KEY,
        title TEXT,
        artist_id TEXT references artists,
        year INT,
        duration NUMERIC(6, 3)
    );
    """
    )

# How to extract int values from timestamp?
# day is like 11th of november == 11
time_table_create = (
    """
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMPTZ PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT
    );
    """
    )

# Fact table is last due to references constraints
# think do_datepart
# or postgres https://tinyurl.com/rplkxo9 https://tinyurl.com/t6jn5wc
# removed references users from user_id
songplay_table_create = (
    """
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id INT GENERATED BY DEFAULT AS IDENTITY UNIQUE,
        start_time TIMESTAMPTZ references time,
        user_id INT,
        level VARCHAR(10),
        session_id INT,
        location TEXT,
        user_agent TEXT,
        artist_id TEXT references artists,
        song_id TEXT references songs
    );
    """
    )

# INSERT RECORDS for df.iterrows() method
# woudl f string work? f"""INSERT INTO songplays({sp_columns}) ?
songplay_table_insert = (
    """
    INSERT INTO songplays(start_time, user_id, level, session_id, location, user_agent, artist_id, song_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    )

user_table_insert = (
    """
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    VALUES (%s, %s, %s, %s, %s);
    """
    )

song_table_insert = (
    """
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    VALUES (%s, %s, %s, %s, %s);
    """
    )

artist_table_insert = (
    """
    INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    VALUES (%s, %s, %s, %s, %s);
    """
    )


time_table_insert = (
    """
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """
    )


# INSERT RECORDS for extras.execute_values() method
songplay_table_query = (
    """
    INSERT INTO songplays(start_time, user_id, level, session_id, location, user_agent, artist_id, song_id)
    VALUES %s;
    """
    )

user_table_query = (
    """
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    VALUES %s;
    """
    )

song_table_query = (
    """
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    VALUES %s;
    """
    )

artist_table_query = (
    """
    INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
    VALUES %s;
    """
    )


time_table_query = (
    """
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    VALUES %s;
    """
    )


# FIND SONGS
'''
song_select = (
    """
    SELECT s.song_id, s.artist_id
    FROM songs s, artists a
    WHERE s.artist_id = a.artist_id;
    """)
'''
# Need to do with song title, artist name and duration
# Values are based off iterating rows in df from log_data
song_select = (
    """
    SELECT s.song_id, s.artist_id
    FROM songs s
    JOIN artists a USING (artist_id)
    WHERE (a.artist_name, s.title, s.duration)
    = (%s, %s, %s);
    """ 
    )


# QUERY LISTS

# Changed order to match or references gives error
create_table_queries = [user_table_create,
                        artist_table_create,
                        song_table_create,
                        time_table_create,
                        songplay_table_create]

drop_table_queries = [songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]