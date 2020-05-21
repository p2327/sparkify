import glob
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from sql_queries import (songplay_table_query,
                         user_table_query,
                         song_table_query,
                         artist_table_query,
                         time_table_query)
from typing import Tuple, List, Any


# Data for pipeline functions
song_columns = ['song_id',
                'title',
                'artist_id',
                'year',
                'duration']


artist_columns = ['artist_id',
                  'artist_name',
                  'artist_location',
                  'artist_latitude',
                  'artist_longitude']


time_columns = ['start_time',
                'hour',
                'day',
                'week',
                'month',
                'year',
                'weekday']


song_artist_columns = ['artist_id',
                       'song_id',
                       'title']


user_columns = ['userId',
                'firstName',
                'lastName',
                'gender',
                'level']


songplays_columns = ['start_time',
                     'userId',
                     'level',
                     'sessionId',
                     'location',
                     'userAgent',
                     'song']


# Type aliases
ArtistData = Tuple[str, str, str, float, float]
SongData = Tuple[str, str, str, int, float]
SongArtistData = Tuple[str, str, str, str, str, float, float]
SongArtistFileData = Tuple[str, str, str]
TimeData = Tuple[str, int, int, int, int, int, int]
UserData = Tuple[int, str, str, str, str]
SongplaysData = Tuple[str, int, str, int, str, str, str]


# Database load functions
def load_song_data(cur, files: List[str]) -> None:
    artist_data, song_data = process_song_files(files)
    execute_values(cur, artist_table_query, artist_data)
    execute_values(cur, song_table_query, song_data)


def load_log_data(cur, files: List[str], support_data: List[str]) -> None:
    time_data, user_data, songplays_data = process_log_files(files,
                                                             support_data)
    execute_values(cur, user_table_query, user_data)
    execute_values(cur, time_table_query, time_data)
    execute_values(cur, songplay_table_query, songplays_data)


# Helper functions
def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    return all_files


def dedupe(data: List[Any]) -> List[Any]:
    data_set = set(tuple(x) for x in data)
    data_deduped = [list(x) for x in data_set]
    return data_deduped


# Song data pipelines
def process_song_files(files: List[str]) -> Tuple[ArtistData, SongData]:
    artist_data = []
    song_data = []

    for file in files:
        artist_data.append(artist_pipeline(file))
        song_data.append(song_pipeline(file))

    artist_data = dedupe(artist_data)
    song_data = dedupe(song_data)
    #print(artist_data)

    return artist_data, song_data


def artist_pipeline(file) -> ArtistData:
    file_data = (pd.read_json(file, lines=True)
                   .loc[:, artist_columns]
                   # rounding for SCHEMA NUMERIC(6, 3) CONSTRAINT
                   .applymap(lambda x: round(x, 3) if isinstance(x, float) else x)
                   .fillna(0)
                   .to_numpy()
                   .ravel()
                   .tolist())
    return file_data


def song_pipeline(file) -> List[str]:
    file_data = (pd.read_json(file, lines=True)
                   .loc[:, song_columns]
                   # rounding for SCHEMA NUMERIC(6, 3) CONSTRAINT
                   .applymap(lambda x: round(x, 3) if isinstance(x, float) else x)
                   .to_numpy()
                   .ravel()
                   .tolist())
    return file_data


def process_song_artist_files(files: List[str]) -> SongArtistData:
    song_artist_data = []
    for file in files:
        song_artist_data.append(song_artist_pipeline(file))

    song_artist_data = dedupe(song_artist_data)
    #print(song_artist_data)

    return song_artist_data


def song_artist_pipeline(file) -> SongArtistFileData:
    file_data = (pd.read_json(file, lines=True)
                   .loc[:, song_artist_columns]
                   .to_numpy()
                   .ravel()
                   .tolist())
    return file_data


# Log data pipelines
def process_log_files(files: List[str], support_data: List[str]
                      ) -> Tuple[TimeData, UserData, SongplaysData]:
    time_data = []
    user_data = []
    songplays_data = []
    for file in files:
        time_data.append(time_pipeline(file))
        user_data.append(user_pipeline(file))
        songplays_data.append(songplays_pipeline(file))

    # Helper functions
    def flatten_log_data(data):
        #flat_data = [item for logfile in data for item in logfile]
        #dedup_data = list(flat_data for flat_data, _
                          #in itertools.groupby(flat_data))
        return [item for logfile in data for item in logfile]

    def process_songplays_data(data_for_songplays, songs_dict):
        processed_data = []
        for entry in data_for_songplays:
            if entry[6] in songs_dict.keys():
                # print(f'{entry[6]} OK')
                entry.extend(songs_dict[entry[6]])
                entry.remove(entry[6])
                processed_data.append(entry)
            else:
                # print(f'{entry[6]} not in songs')
                entry.extend([None, None])
                entry.remove(entry[6])
                processed_data.append(entry)

        return processed_data

    # Create a dictionary to match songs in log data to songs_artists data
    song_artist_data = process_song_artist_files(support_data)
    song_artist_keys = [item[2] for item in song_artist_data]
    song_artist_values = [item[0:2] for item in song_artist_data]
    song_artist_dispatch = dict(zip(song_artist_keys, song_artist_values))

    # Flatten and dedupe data
    songplays_data_processed = dedupe(flatten_log_data(songplays_data))
    user_data_processed = dedupe(flatten_log_data(user_data))
    time_data_processed = dedupe(flatten_log_data(time_data))
    # print(f'#######\n{len(songplays_data_flat)}\n#######')
    # print(f'#######\n{len(user_data_flat)}\n#######')
    # print(f'#######\n{len(time_data_flat)}\n#######')

    # Process songplays data to add song_id and artist_id
    songplays_data_processed = process_songplays_data(songplays_data_processed,
                                                      song_artist_dispatch)

    # Replace '' with None for users missing ids for postgres ingestion
    #user_data_processed = [[-1 if x == '' else x for x in item]
                            #for item in user_data_processed]

    # Instead directly remove data with muissing user_id
    # This way unique constraint for PRIMARY KEY is repsected
    user_data_processed = [item for item in user_data_processed if item[0] != '']

    return time_data_processed, user_data_processed, songplays_data_processed


def time_pipeline(file) -> TimeData:
    file_data = (pd.read_json(file,
                              convert_dates=['registration', 'ts'],
                              date_unit='ms',
                              lines=True)
                   .query("song.to_numpy() != None")
                   .assign(start_time=lambda x: x.ts)
                   .assign(hour=lambda x: x.ts.dt.hour)
                   .assign(day=lambda x: x.ts.dt.day)
                   .assign(week=lambda x: x.ts.dt.weekofyear)
                   .assign(month=lambda x: x.ts.dt.month)
                   .assign(year=lambda x: x.ts.dt.year)
                   .assign(weekday=lambda x: x.ts.dt.weekday)
                   #.applymap(lambda t: t.strftime('%Y-%m-%d %H:%M:%f') if isinstance(t, pd.Timestamp) else t)
                   .loc[:, time_columns]
                   .to_numpy()
                   .tolist())

    return file_data


def user_pipeline(file) -> UserData:
    file_data = (pd.read_json(file,
                              convert_dates=['registration', 'ts'],
                              date_unit='ms',
                              lines=True)
                   .loc[:, user_columns]
                   # Cast to int for first item as log data has str in some entries
                   .applymap(lambda x: int(x) if str(x).isnumeric() else x)
                   .to_numpy()
                   .tolist())
    return file_data


def songplays_pipeline(file) -> SongplaysData:
    file_data = (pd.read_json(file,
                              convert_dates=['registration', 'ts'],
                              date_unit='ms',
                              lines=True)
                   .query("song.to_numpy() != None")
                   .assign(start_time=lambda x: x.ts)
                   #.applymap(lambda t: t.strftime('%Y-%m-%d %H:%M:%f') if isinstance(t, pd.Timestamp) else t)
                   .loc[:, songplays_columns]
                   # Cast to int for first item as log data has str in some entries
                   .applymap(lambda x: int(x) if str(x).isnumeric() else x)
                   .to_numpy()
                   .tolist())

    return file_data


def process_data(cur, conn, filepath, support_data, func):
    # Get all files matching extension from directory
    all_files = get_files(filepath)

    # Get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # Process files
    if support_data:
        print(f'This executed for {func}')
        song_files = get_files('data/song_data')
        func(cur, all_files, song_files)
        conn.commit()
    else:
        print(f'This executed for {func}')
        func(cur, all_files)
        conn.commit()
    
    #conn.commit()


def main():
    conn = psycopg2.connect(
        "host=127.0.0.1 dbname=sparkifydb user=postgres password=Ckmerrypos44!"
        )
    cur = conn.cursor()

    process_data(cur,
                 conn,
                 filepath='data/song_data',
                 support_data=False,
                 func=load_song_data)

    process_data(cur,
                 conn,
                 filepath='data/log_data',
                 support_data=True,
                 func=load_log_data)

    conn.close()


if __name__ == "__main__":
    main()
