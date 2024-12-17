import os
import sys
from pathlib import Path
import json
import sqlite3


if __name__ == '__main__':
    dbpath = Path(__file__).parent / 'blobby.db'
    print(dbpath)
    if dbpath.exists():
        dbpath.unlink()
    conn = sqlite3.connect(dbpath)
    sql = 'CREATE TABLE test (mycolumn blob)'    
    conn.execute(sql)
    sql = 'INSERT INTO test (mycolumn) VALUES (?)'
    data = {}
    data['hi'] = 'world'
    data = json.dumps(data).encode('utf8')
    print(type(data))
    quit()
    params = [data]
    conn.execute(sql, params)
    conn.commit()

    # select
    sql = 'SELECT mycolumn FROM test;'
    cursor = conn.cursor()
    res = cursor.execute(sql)
    for row in res:
        print(row)
    conn.close()
