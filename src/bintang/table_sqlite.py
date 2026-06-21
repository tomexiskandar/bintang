import sqlite3
import json
from datetime import datetime
from dateutil import parser
from bintang.log import log
from bintang.cell import Cell
from bintang.row import Row
from bintang.table_base import Base_Table

class DateTimeJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.parse_dates, *args, **kwargs)

    def parse_dates(self, obj):
        for key, value in obj.items():
            if isinstance(value, str):
                try:
                    # dateutil.parser is great, but we want to be careful 
                    # not to accidentally parse regular strings as dates.
                    # Checking for a hybrid of digits and dashes/colons helps.
                    if any(char in value for char in ['-', ':']) and any(c.isdigit() for c in value):
                        obj[key] = parser.isoparse(value)  # Fast for ISO strings
                except (ValueError, TypeError):
                    try:
                        obj[key] = parser.parse(value)  # Fallback for fuzzy dates
                    except (ValueError, TypeError):
                        pass  # Not a date, leave as string
        return obj

def adapt_json(data):
    # Convert Python dict/list to JSON string to store in DB
    return json.dumps(data, default=str)

def convert_json(blob):
    # Convert JSON string from DB back to Python dict with parsed datetimes
    return json.loads(blob.decode('utf-8'), cls=DateTimeJSONDecoder)

# Registering the handlers
sqlite3.register_adapter(dict, adapt_json)
sqlite3.register_adapter(list, adapt_json)
sqlite3.register_converter("JSON_DATA", convert_json)

class Sqlite_Table(Base_Table):
    def __init__(self, name, conn, bing):
        super().__init__(name, bing=bing)
        self.conn = conn  # for backend use
        self.conn.row_factory = sqlite3.Row
        self._create_sql_table()


    def __len__(self):
        """ return the length of rows"""
        cursor = self.conn.cursor()
        res = cursor.execute(f'SELECT COUNT(*) FROM {self.name};')
        return res.fetchone()[0]      


    def _create_sql_table(self):
        cur = self.conn.cursor()
        cur.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name=:tablename)", {"tablename":self.name})
        ret = cur.fetchone()[0]
        if  ret == 0:
            # cur.execute(f"CREATE TABLE '{self.name}' (id INTEGER PRIMARY KEY NOT NULL, cells JSON)")
            # cur.execute(f"CREATE TABLE '{self.name}__columns__' (id INTEGER PRIMARY KEY NOT NULL, name TEXT COLLATE NOCASE, ordinal_position INTEGER, data_type TEXT, column_size INTEGER, decimal_digits INTEGER, data_props JSON)")
            cur.execute(f"CREATE TABLE '{self.name}' (cells JSON_DATA)")
            cur.execute(f"CREATE TABLE '{self.name}__columns__' (columnid INTEGER PRIMARY KEY NOT NULL, name TEXT COLLATE NOCASE, ordinal_position INTEGER, data_type TEXT, column_size INTEGER, decimal_digits INTEGER, data_props JSON)")
        cur.close()

    
    def get_columnid(self, column):
        sql = f"SELECT columnid FROM {self.name}__columns__ where name = ?"
        cursor = self.conn.cursor()
        res = cursor.execute(sql,(column,))
        ret = res.fetchone()
        if ret:
            return ret['columnid']
        else:
            return None
        

    def get_columnids(self, columns=None):
        if columns is None:
            columns = self.get_columns()
        sql = f"SELECT columnid FROM {self.name}__columns__ where name IN ({','.join(['?']*len(columns))})"
        cursor = self.conn.cursor()
        res = cursor.execute(sql, columns)
        ret = res.fetchall()
        if ret:
            return [r['columnid'] for r in ret]
        else:
            return None

    
    def _get_last_assigned_ord_pos(self) -> int:
        cursor = self.conn.cursor()
        sql = f"SELECT MAX(ordinal_position) AS MAXOF_ORD_POS FROM {self.name}__columns__;"
        res = cursor.execute(sql)
        ret = res.fetchone()
        if ret:
            if ret['MAXOF_ORD_POS'] is None: # first function call
                return -1
            else:
                return ret['MAXOF_ORD_POS']
        else:
            return -1
    
    
    def _add_column_sql_(self, name, data_type=None, column_size=None):
        # check if the passed name already exists
        columnid = self.get_columnid(name)
        ord_pos = self._get_last_assigned_ord_pos() + 1
        if columnid is None:
            sql = f"INSERT INTO {self.name}__columns__ (name, ordinal_position, data_type, column_size) VALUES (?,?,?,?)"
            params = [name, ord_pos, data_type, column_size]
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            self.conn.commit()
            cursor.close()
        else:
            log.debug(f'Warning! trying to add existing column "{name}".')

    
    def _make_cell_sql(self,column,value,new_column=True):
        columnid = self.get_columnid(column)
        if columnid is None: # if columnid is None then assume user wants a new column
            if new_column == True:
                self._add_column_sql_(column)
                columnid = self.get_columnid(column) # reassign the columnid
        if columnid is None:
            raise ValueError("Cannot make cell due to None column name.")    
        return Cell(columnid,value) 

    
    def _add_row_sql(self, row, index=None):
        cur = self.conn.cursor()
        # sql = f'INSERT INTO "{self.name}" (id, cells) VALUES (?,?)'
        # cur.execute(sql, (row.id, row.cells)) 
        sql = f'INSERT INTO "{self.name}" (cells) VALUES (?)'
        cur.execute(sql, (row.cells,)) 


    def get_columns(self):
        cursor = self.conn.cursor()
        sql = f"SELECT name FROM {self.name}__columns__ order by ordinal_position;"
        sorted_columns = []
        for row in cursor.execute(sql):
            sorted_columns.append(row['name'])
        return sorted_columns


    def _order_columns_sql(self, columns):
        # determine all columns
        columns_all = [x for x in columns]
        for col in self._get_columns_sql():
            if col not in columns_all:
                columns_all.append(col)
        for i, col in enumerate(columns_all, 10):
            self._update_column_sql(col, ordinal_position=i)


    def _update_column_sql(self,name, data_type=None, column_size=None, ordinal_position=None):
        # check if the passed name already exists
        columnid = self.get_columnid(name)
        if columnid is not None:
            cursor = self.conn.cursor()
            if data_type is not None:
                sql = f"UPDATE {self.name}__columns__ SET data_type=? WHERE id=?;"
                params = [data_type, columnid]
                cursor.execute(sql,params)
            if column_size is not None:
                sql = 'UPDATE {self.name}__columns__ SET column_size=? WHERE id=?;'
                params = [column_size, columnid]
                cursor.execute(sql,params)
            if ordinal_position is not None:
                sql = 'UPDATE {self.name}__columns__ SET ordinal_position=? WHERE id=?;'
                params = [ordinal_position, columnid]
                cursor.execute(sql,params)           






    def insert(self, dict_or_columns, values=None, index=None):
        """ restrict arguments for dict_or_columns insertion for this function as the followings:
        1. a dictionary pass to dict_or_columns param
        2. list values pass to dict_or_columns param and list columns pass to column param.
        """
        if isinstance(dict_or_columns, dict):
            #row = self.make_row()
            row = self.make_row() #Row(0)# dummy idx
            for idx, (col, val) in enumerate(dict_or_columns.items()):
                cell = self._make_cell_sql(col, val)
                row.add_cell(cell) # add to rows
            row.cells = json.dumps({v.columnid: v.value for v in row.cells.values()}, default=str)
            self._add_row_sql(row, index)
            
                                                  
        elif isinstance(dict_or_columns,list) or isinstance(dict_or_columns,tuple) or isinstance(dict_or_columns,list) or isinstance(dict_or_columns,tuple):
            #row = self.make_row()
            row = self.make_row() #Row(0) # dummy idx
            for idx, col in enumerate(dict_or_columns):
                cell = self._make_cell_sql(col,values[idx])
                row.add_cell(cell) # add to rows
            row.cells = json.dumps({v.columnid: v.value for v in row.cells.values()}, default=str)
            self._add_row_sql(row, index)
        else:
            raise ValueError("Arg for dict_or_columns set for dictionary or list/tuple of values with list/tuple of columns.")

    
    def _iterrows(self):
        cur = self.conn.cursor()
        sql = "SELECT ROWID, cells FROM {}".format(self.name) # extract rowid as well
        for sqlrow in cur.execute(sql):
            row = Row(sqlrow['ROWID']) # create row object with rowid as id
            #for columnid, value in json.loads(sqlrow['cells']).items(): # depreacated as we don't need this as the converter is in registered in connection.
            for columnid, value in sqlrow['cells'].items(): 
                cell = Cell(int(columnid), value) # convert columnid to int as it is stored as string in json
                row.add_cell(cell)
            yield sqlrow['ROWID'], row
    
    def _get_columns_withid_sql(self) -> dict:
        # get columnnames from db and return as dict of columnid : columnname
        col_dict = {}
        cur = self.conn.cursor()
        sql = f"SELECT columnid, name FROM {self.name}__columns__ order by ordinal_position;"
        for row in cur.execute(sql):
            col_dict[row['name']] = row['columnid'] 
        return col_dict


    def _gen_cells_dict(self, cells: str) -> dict:
        return {int(k):v for k,v in json.loads(cells).items()}    
    
    
    def _gen_row_asdict_sql(self, cells: str, columns: list) -> dict:
        db_cols_withid = self._get_columns_withid_sql()
        user_cols = {k:v for k,v in db_cols_withid.items() if k in columns}
        cells_dict = self._gen_cells_dict(cells)
        row_dict = {}
        for col in columns:
            if user_cols[col] in cells_dict:
                row_dict[col] = cells_dict[user_cols[col]] 
            else:
                row_dict[col] = None
        return row_dict


    def _iter_row_asdict_sql(self,colid_dict):
        for idx, row in self._iterrows():
            row_asdict = {}
            for col, id in colid_dict.items():
                if id in row.cells:
                    row_asdict[col] = row.cells[id].value
                else:
                    row_asdict[col] = None
            yield idx, row_asdict


    def _iter_row_aslist_sql(self,colid_dict):
        for idx, row in self._iterrows():
            row_aslist = []
            for col, id in colid_dict.items():
                if id in row.cells:
                    row_aslist.append(row.cells[id].value)
                else:
                    row_aslist.append(None) 
            yield idx, row_aslist
    

    def iterrows(self, 
                 columns: list=None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: bool=False):

        # validate columns, if column is None then assign all available columns, otherwise validate the passed columns.
        db_cols_withid = self._get_columns_withid_sql()
        if columns is None:
            columns = self.get_columns() # assign all available column names  
            colid_dict = {k:v for k,v in db_cols_withid.items() if k in columns} #refine columns
        else:
            # validate passed columns
            columns = self.validate_columns(columns) 
            colid_dict = {k:self.get_columnid(k) for k in columns}

        if row_type == 'list':
            for idx, row in self._iter_row_aslist_sql(colid_dict):
                yield idx, row 
                    
        else: # assume row_type is dict 
            for idx, row in self._iter_row_asdict_sql(colid_dict):
                yield idx, row
    
    
    def get_row_asdict(self, idx, columns=None):
        if columns is None:
            columns = self.get_columns() # assign all available column names
        else:
            columns = self.validate_columns(columns) # assign all available column names after validation
        cursor = self.conn.cursor()
        sql = "SELECT cells FROM {} WHERE ROWID=?".format(self.name)
        res = cursor.execute(sql, (idx,))
        ret = res.fetchone()
        if ret:
            return self._gen_row_asdict_sql(ret['cells'], columns)
        else:
            return None


    def get_row_aslist(self, idx, columns=None):
        if columns is None:
            columns = self.get_columns() # assign all available column names
        else:
            columns = self.validate_columns(columns) # assign all available column names after validation
        cursor = self.conn.cursor()
        sql = "SELECT cells FROM {} WHERE ROWID=?".format(self.name)
        res = cursor.execute(sql, (idx,))
        ret = res.fetchone()
        if ret:
            return list(self._gen_row_asdict_sql(ret['cells'], columns).values())
        else:
            return None                

    
    def update_row(self, idx, column, value):
        """ using json_set so only update for a specific key"""
        cursor = self.conn.cursor() 
        columnid = self.get_columnid(column)
        sql_update = f"UPDATE {self.name} SET cells = (SELECT json_set({self.name}.cells, '$.{columnid}', ?)) where ROWID=?;"
        params = [value, idx]
        cursor.execute(sql_update, params)


    def _update_row_sql_bycolid(self, idx, columnid, value):
        """ using json_set so only update for a specific key and accept columnid
        Note: no performance increase even not calling get_columnid_sql()"""
        cursor = self.conn.cursor() 
        sql_update = f"UPDATE {self.name} SET cells = (SELECT json_set({self.name}.cells, '$.{columnid}', ?)) where ROWID=?;"
        params = [value, idx]
        cursor.execute(sql_update, params)
        
                            

