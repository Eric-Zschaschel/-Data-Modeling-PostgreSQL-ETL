import os
import io
import glob
import psycopg2
import pandas as pd
from typing import Iterator, Dict, Any, Optional

# get all files matching extension from directory
def get_files(filepath):
    """Returns list of the pathnames of the json files."""
    all_files = []
    # walk() generates the file names in a directory tree
    for root, dirs, files in os.walk(filepath):
        # glob finds all the pathnames matching a specified pattern
        # join combines the two path elements
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            # add the absolute path to the list
            all_files.append(os.path.abspath(f))
    return all_files


def clean_csv_value(value: Optional[Any]) -> str:
    r""" 
    Empty values are transformed to \N. 
    The string \N is the default string used by PostgreSQL to indicate 
    NULL in COPY (this can be changed using the NULL option).
    
    """
    if value is None:
        return r'\N'
    if value == 'NaN':
        return r'\N'        
    
    return str(value)


class StringIteratorIO(io.TextIOBase):
    r"""
    this class is copied from here: 
    https://hakibenita.com/fast-load-data-python-postgresql#copy-data-from-a-string-iterator
    
    thanks to the beer iterator the following class creates a file-like object that will act as a 
    buffer between the remote source and the COPY command. The buffer will consume 
    JSON via the iterator, clean and transform the data, and output clean CSV.
    
    """
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)



def json_gen(file_list: list) -> Iterator[Dict[str, Any]]:
    """
    Creates a generator that reads a list of data paths and loads each json file as a 
    dictionary. If a json file has multiple dictionaries inside, it yields them separately.
    
    """
    import json
    for file in file_list:    
        with open(file) as json_file: 
            data = []
            for line in json_file:
                data = json.loads(line)
                if not data:
                    break
                yield data
            
def process_songs(cur, conn, datapath: str) -> None:
    """inserts json data into the songs table"""
    file_list = get_files(datapath)
    jsonfile = json_gen(file_list)
    x = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            i['song_id'],
            i['title'],
            i['artist_id'],
            i['year'],
            i['duration']
        ))) + '\n'
        for i in jsonfile if i['song_id'] != ''
    ))
    cur.execute("""DROP TABLE IF EXISTS tmp_songs;
                SELECT * INTO tmp_songs FROM songs;""")    
    cur.copy_from(x, 'tmp_songs', sep='|')   
    cur.execute("""INSERT INTO songs (song_id, title, artist_id, year, duration)
                SELECT song_id, title, artist_id, year, duration FROM tmp_songs
                ON CONFLICT (song_id) DO NOTHING;
                DROP TABLE tmp_songs;""")
    conn.commit()


def process_artists(cur, conn, datapath: str) -> None:
    """inserts json data into the artists table"""
    file_list = get_files(datapath)
    jsonfile = json_gen(file_list)
    x = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            i['artist_id'],
            i['artist_name'],
            i['artist_location'],
            i['artist_latitude'],
            i['artist_longitude']
        ))) + '\n'
        for i in jsonfile if i['artist_id'] != ''
    ))
    cur.execute("""DROP TABLE IF EXISTS tmp_artists;
                SELECT * INTO tmp_artists FROM artists;""")    
    cur.copy_from(x, 'tmp_artists', sep='|')    
    cur.execute("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
                SELECT artist_id, name, location, latitude, longitude FROM tmp_artists
                ON CONFLICT (artist_id) DO NOTHING;
                DROP TABLE tmp_artists;""")
    conn.commit()

def json_gen_time(file_list: list) -> Dict[str, Any]:
    """
    This Generator iterates over the json files
    and creates dataframes which are filtered, formatted
    and then exported into python dictionaries
    
    """
    for file in file_list:    
        df = pd.read_json(file, lines=True)
        df = df.loc[df['page'] == 'NextSong']
        df['ts']= pd.to_datetime(df['ts'], unit = 'ms')
        
        t = df['ts']
        t.drop_duplicates(inplace=True)
        t.dropna(inplace=True)
        
        time_df = pd.DataFrame(index=t.index)
        time_df['start_time'] = t
        time_df['hour'] = t.dt.hour
        time_df['day'] = t.dt.day
        time_df['week'] = t.dt.weekofyear
        time_df['month'] = t.dt.month
        time_df['year'] = t.dt.year
        time_df['weekday'] = t.dt.weekday

        for index, row in time_df.iterrows():
            data = row.to_dict()
            yield data

def process_time(cur, conn, datapath: str) -> None:
    """inserts json data into the time table"""
    file_list = get_files(datapath)
    jsonfile = json_gen_time(file_list)
    x = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            i['start_time'],
            i['hour'],
            i['day'],
            i['week'],
            i['month'],
            i['year'],
            i['weekday']
        ))) + '\n'
        for i in jsonfile if i['start_time'] != ''
    ))
    cur.execute("""DROP TABLE IF EXISTS tmp_time;
                SELECT * INTO tmp_time FROM time;""")
    cur.copy_from(x, 'tmp_time', sep='|')
    cur.execute("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
                SELECT start_time, hour, day, week, month, year, weekday FROM tmp_time
                ON CONFLICT (start_time) DO NOTHING;
                DROP TABLE tmp_time;""")
    conn.commit()

def process_users(cur, conn, datapath: str) -> None:
    """inserts json data into the users table"""
    file_list = get_files(datapath)
    jsonfile = json_gen(file_list)
    x = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            i['userId'],
            i['firstName'],
            i['lastName'],
            i['gender'],
            i['level']
        ))) + '\n'
        for i in jsonfile if i['userId'] != ''
    ))
    cur.execute("""DROP TABLE IF EXISTS tmp_users;
                SELECT * INTO tmp_users FROM users;""")
    cur.copy_from(x, 'tmp_users', sep='|')
    cur.execute("""INSERT INTO users (user_id, first_name, last_name, gender, level)
                SELECT user_id, first_name, last_name, gender, level FROM tmp_users 
                WHERE user_id IS NOT NULL AND user_id <> 0
                ON CONFLICT (user_id) DO NOTHING;
                DROP TABLE tmp_users;""")
    conn.commit()


def process_songplays(cur, conn, datapath: str) -> None:
    """
    After some A/B testing I figured out that a query for songid and artistid 
    inside the StringIterator x does not work as x will be used in copy_from.
    
    My solution: Doing the filtering after copy_from.
    I also widened the songlist table for analytical purposes.
    
    """ 
    from datetime import datetime
    file_list = get_files(datapath)
    jsonfile = json_gen(file_list)
    
    def string_iterator(jsonfile):
        for i in jsonfile:
            if not i['userId']:
                continue
            if not i['ts']:
                continue
           
            y = '|'.join(map(clean_csv_value, (
                datetime.fromtimestamp(i['ts']/1000.0),
                i['userId'],
                i['level'],
                r'\N', #songid
                r'\N', #artistid
                i['sessionId'], 
                i['location'],
                i['userAgent'],
                i['song'],
                i['artist'],
                i['length']
            ))) + '\n'
            yield y
   
    x = StringIteratorIO(string_iterator(jsonfile))
    cur.execute("""DROP TABLE IF EXISTS tmp_songplays;
                ALTER TABLE songplays 
                ADD COLUMN song VARCHAR,
                ADD COLUMN artist VARCHAR, 
                ADD COLUMN length DOUBLE PRECISION;
                SELECT * INTO tmp_songplays FROM songplays; 
                """)
    cur.copy_from(x, 'tmp_songplays', sep='|')
    cur.execute("""INSERT INTO songplays (start_time, user_id, level, 
                song_id, artist_id, session_id, location, user_agent, 
                song, artist, length) 
                SELECT g.start_time, g.user_id, g.level, h.song_id, 
                h.artist_id, g.session_id, g.location, g.user_agent, 
                g.song, g.artist, g.length FROM tmp_songplays g
                LEFT JOIN (
                    SELECT song_id, j.artist_id, k.name as artist, title, 
                    duration FROM songs j INNER JOIN artists k 
                    ON j.artist_id = k.artist_id) h ON g.song = h.title 
                    AND g.length = h.duration AND g.artist = h.artist
                ON CONFLICT (start_time, user_id) DO NOTHING;
                DROP TABLE tmp_songplays;""")
    conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    filepath = os.path.normcase(os.getcwd()) + '/data/song_data'
    process_songs(cur, conn, filepath)
    process_artists(cur, conn, filepath)
    
    filepath = os.path.normcase(os.getcwd()) + '/data/log_data'
    process_time(cur, conn, filepath)
    process_users(cur, conn, filepath)
    process_songplays(cur, conn, filepath)
    
    conn.close()


if __name__ == "__main__":
    main()



