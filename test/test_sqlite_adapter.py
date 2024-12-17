import sqlite3
from bintang.table import Table
from bintang.column import Column
from bintang.cell import Cell
#from bintang.row import Row

class Point:
    def __init__(self, x, y):
        self.x, self.y = x, y

    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return f"{self.x};{self.y}"

class Row:
    """Define a row as a collection of cells
    """ 
    def __init__(self,id):
        self.id = id
        self.cells = {}

    def __conform__(self, protocol):
        if protocol is sqlite3.PrepareProtocol:
            return '{}(id:{}, cells:{})'.format(__class__.__name__, self.id, self.cells)

con = sqlite3.connect(":memory:")
cur = con.cursor()

# cur.execute("SELECT ? as mycolumn", (Point(4.0, -3.2),))
# print(cur.description)
# print(cur.fetchone()[0])


#cur.execute("SELECT ? as mycolumn", (Row(1),))
cur.execute("CREATE TABLE test (mycol row)")
row = Row(1)
cur.execute("INSERT INTO test (mycol) VALUES (?)", (row,))
#print(cur.fetchone()[0])
cur.execute("SELECT mycol FROM test;")
print(cur.fetchone()[0])