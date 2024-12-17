import sqlite3

if __name__ == '__main__':
    print('in main')
    conn = sqlite3.connect('example.db')
    cur = conn.cursor()
    
