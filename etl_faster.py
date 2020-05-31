import os
import io
import glob
import psycopg2
import pandas as pd
from typing import Iterator, Dict, Any, Optional
from sql_queries import insert_queries


"""
The official documentation for PostgreSQL features an entire section on
Populating a Database. According to the documentation, the best way to load
data into a database is using the copy command - this is much faster than the
INSERT. Therefore I created this etl to do exactly that.

"""

# get all files matching extension from directory
def get_files(filepath):
    """Returns list of the pathnames of the json files. Copied from etl.py"""
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
    Empty values are transformed to \N. It is the default string
    used by PostgreSQL to indicate NULL in COPY (this can be changed
    using the NULL option).
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


def process_song_data(cur, conn, datapath: str) -> None:
    """Loads song json data into a staging table"""
    file_list = get_files(datapath)
    jsonfile = json_gen(file_list)
    x = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            i['song_id'],
            i['title'],
            i['artist_id'],
            i['year'],
            i['duration'],
            i['artist_name'],
            i['artist_location'],
            i['artist_latitude'],
            i['artist_longitude']
        ))) + '\n'
        for i in jsonfile if i['song_id'] != ''
    ))
    cur.execute("""DELETE FROM staging_songs;""")
    cur.copy_expert("COPY staging_songs FROM STDIN DELIMITER '|'", x)
    conn.commit()


def process_event_data(cur, conn, datapath: str) -> None:
    """Loads event json data into a staging table"""
    from datetime import datetime
    file_list = get_files(datapath)
    jsonfile = json_gen(file_list)
    x = StringIteratorIO((
        '|'.join(map(clean_csv_value, (
            datetime.fromtimestamp(i['ts']/1000.0),
            i['userId'],
            i['level'],
            i['sessionId'],
            i['location'],
            i['userAgent'],
            i['firstName'],
            i['lastName'],
            i['gender'],
            i['song'],
            i['artist'],
            i['length']
        ))) + '\n'
        for i in jsonfile if i['userId'] != ''
    ))
    cur.execute("""DELETE FROM staging_events;""")
    cur.copy_expert("COPY staging_events FROM STDIN DELIMITER '|'", x)
    conn.commit()


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=great")
    cur = conn.cursor()

    datapath = os.path.normcase(os.getcwd()) + '/data/log_data'
    process_event_data(cur, conn, datapath)

    datapath = os.path.normcase(os.getcwd()) + '/data/song_data'
    process_song_data(cur, conn, datapath)

    for query in insert_queries:
        cur.execute(query)
        conn.commit()

    conn.close()


if __name__ == "__main__":
    main()
