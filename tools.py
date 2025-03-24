
from os.path import isfile
from os import access, R_OK
import datetime as dt
import pandas as pd 

def get_dict(filename):

    assert isfile(filename), f"File not found: '{filename}'"
    assert access(filename, R_OK), f"File '{filename}' doesn't exist or isn't readable"

    f = open(filename)

    content = f.read().splitlines() 

    f.close()

    isFindComment = True

    keys = []
    queries = []

    while len (content) > 0:

        c = content.pop(0)

        if len(c) == 0:
            continue

        isComment = c.startswith("/*") and c.endswith("*/")

        isSQL = c.endswith(';')

        if isComment and isFindComment:
            c = c[2:]
            c = c[:-2]

            query_string = c.split(':')

            sql_key = query_string[0]

            keys.append(sql_key)

            isFindComment = False

        elif isSQL and not isFindComment:
            c = c[:-1]
            queries.append(c)
            isFindComment = True
            
    if len(keys) > len (queries):
        keys.pop()

    assert len(keys) == len (queries)

    d = dict(zip(keys, queries))

    return d



def get_date_formats(df, timestamp_col):
   
    df['h'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%H')
    df['d'] = df[timestamp_col].astype('datetime64[ns]').dt.date
    df['dw'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%A')
    df['dm'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%d')
    df['yw'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%Y-%W')
    df['ym'] = df[timestamp_col].astype('datetime64[ns]').dt.strftime('%Y-%m')
    
    return df



