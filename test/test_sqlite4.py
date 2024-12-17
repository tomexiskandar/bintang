# this is a wrapper of sqlite3, not the real sqlite4
import sqlite3
sqlite3.connect("20231215_testdb.db")
data = {'name':'zeke'}
pdata = cPickle.dumps(data, cPickle.HIGHEST_PROTOCOL)
curr.execute("insert into table (data) values (:data)", sqlite3.Binary(pdata))
