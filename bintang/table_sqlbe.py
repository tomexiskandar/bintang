import sqlite3
import json
from bintang.log import log
from bintang.cell import Cell
from bintang.row import Row
from bintang.table import Base_Table


class SQL_Backend_Table(Base_Table):
    def __init__(self, name, conn, bing):
        super().__init__(name, bing=bing)
        self.type = 'SQLBACKEND'  # type of table
        self.conn = conn  # for backend use
        self.conn.row_factory = sqlite3.Row
        self.create_sql_table()


    def __len__(self):
        """ return the length of rows"""
        cursor = self.conn.cursor()
        res = cursor.execute(f'SELECT COUNT(*) FROM {self.name};')
        return res.fetchone()[0]      


    def create_sql_table(self):
        cur = self.conn.cursor()
        cur.execute("SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type='table' AND name=:tablename)", {"tablename":self.name})
        ret = cur.fetchone()[0]
        if  ret == 0:
            cur.execute("CREATE TABLE '{}' (idx INTEGER PRIMARY KEY NOT NULL, cells JSON)".format(self.name))
            cur.execute("CREATE TABLE '__columns__' (id INTEGER PRIMARY KEY NOT NULL, name TEXT COLLATE NOCASE, ordinal_position INTEGER, data_type TEXT, column_size INTEGER, decimal_digits INTEGER, data_props JSON)")
        cur.close()

    
    def _get_columnid_sql(self, column):
        sql = 'SELECT id FROM __columns__ where name = ?'
        cursor = self.conn.cursor()
        res = cursor.execute(sql,(column,))
        ret = res.fetchone()
        if ret:
            return ret['id']
        else:
            return None

    
    def _get_last_assigned_ord_pos(self) -> int:
        cursor = self.conn.cursor()
        sql = 'SELECT MAX(ordinal_position) AS MAXOF_ORD_POS FROM __columns__;'
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
        columnid = self._get_columnid_sql(name)
        ord_pos = self._get_last_assigned_ord_pos() + 1
        if columnid is None:
            sql = "INSERT INTO __columns__ (name, ordinal_position, data_type, column_size) VALUES (?,?,?,?)"
            params = [name, ord_pos, data_type, column_size]
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            self.conn.commit()
            cursor.close()
        else:
            log.debug(f'Warning! trying to add existing column "{name}".')

    
    def _make_cell_sql(self,column,value,new_column=True):
        columnid = self._get_columnid_sql(column)
        if columnid is None: # if columnid is None then assume user wants a new column
            if new_column == True:
                self._add_column_sql_(column)
                columnid = self._get_columnid_sql(column) # reassign the columnid
                # if self.__be is not None:
                #     self.__be.add_column(self.name, columnid, column)
                # deprecated moved up columnid = self.get_columnid(column) # reassign the columnid
        if columnid is None:
            raise ValueError("Cannot make cell due to None column name.")    
        return Cell(columnid,value) 

    
    def _add_row_sql(self, row, index=None):
        cur = self.conn.cursor()
        sql = "INSERT INTO '{}' (cells) VALUES (?)".format(self.name)
        log.debug(sql)
        log.debug(row)
        cur.execute(sql, [row])

    
    def _get_columns_sql(self) -> list:
        cursor = self.conn.cursor()
        sql = 'SELECT name FROM __columns__ order by ordinal_position;'
        sorted_columns = []
        for row in cursor.execute(sql):
            sorted_columns.append(row['name'])
        return sorted_columns 


    def get_columns(self):
        return self._get_columns_sql()


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
        columnid = self._get_columnid_sql(name)
        if columnid is not None:
            cursor = self.conn.cursor()
            if data_type is not None:
                sql = 'UPDATE __columns__ SET data_type=? WHERE id=?;'
                params = [data_type, columnid]
                cursor.execute(sql,params)
            if column_size is not None:
                sql = 'UPDATE __columns__ SET column_size=? WHERE id=?;'
                params = [column_size, columnid]
                cursor.execute(sql,params)
            if ordinal_position is not None:
                sql = 'UPDATE __columns__ SET ordinal_position=? WHERE id=?;'
                params = [ordinal_position, columnid]
                cursor.execute(sql,params)           






    def insert(self, record, columns=None, index=None):
        """ restrict arguments for record insertion for this function as the followings:
        1. a dictionary pass to record param
        2. list values pass to record param and list columns pass to column param.
        """
        if isinstance(record, dict):
            #row = self.make_row()
            row = Row(0) # dummy idx
            for idx, (col, val) in enumerate(record.items()):
                cell = self._make_cell_sql(col, val)
                row.add_cell(cell) # add to rows
            row = json.dumps({v.columnid: v.value for v in row.cells.values()})
            self._add_row_sql(row, index)
            
                                                  
        elif isinstance(columns,list) or isinstance(columns,tuple) or isinstance(record,list) or isinstance(record,tuple):
            #row = self.make_row()
            row = Row(0) # dummy idx
            for idx, col in enumerate(columns):
                cell = self._make_cell_sql(col,record[idx])
                row.add_cell(cell) # add to rows
            row = json.dumps({v.columnid: v.value for v in row.cells.values()})
            self._add_row_sql(row, index)
        else:
            raise ValueError("Arg for record set for dictionary or list/tuple of values with list/tuple of columns.")

    
    def _get_columns_withid_sql(self) -> dict:
        # get columnnames from db and return as dict of columnid : columnname
        col_dict = {}
        cur = self.conn.cursor()
        sql = "SELECT id, name FROM __columns__ order by ordinal_position;"
        for row in cur.execute(sql):
            col_dict[row['name']] = row['id'] 
        return col_dict


    def _gen_cells_dict(self, cells: str) -> dict:
        return {int(k):v for k,v in json.loads(cells).items()}    
    
    
    def _gen_row_dict_sql(self, cells: str, columns: list) -> dict:
        db_cols_withid = self._get_columns_withid_sql()
        user_cols = {k:v for k,v in db_cols_withid.items() if k in columns}
        cells_dict = self._gen_cells_dict(cells)
        row_dict = {}
        for col in columns:
            row_dict[col] = cells_dict[user_cols[col]]
        return row_dict


    def _gen_row_asdict_sql(self,columns):
        # get columnames
        db_cols_withid = self._get_columns_withid_sql()
        user_cols = {k:v for k,v in db_cols_withid.items() if k in columns} #refine columns
       
        cur = self.conn.cursor()
        sql = "SELECT idx, cells FROM {}".format(self.name)
        for row in cur.execute(sql):
            # debug cells_dict = json.loads(row["cells"])
            # log.debug(cells_dict)
            cells_dict = self._gen_cells_dict(row['cells'])
            row_asdict = {}
            for col in columns:
                if user_cols[col] in cells_dict:
                    row_asdict[col] = cells_dict[user_cols[col]]
                else:
                    row_asdict[col] = None
            yield row["idx"], row_asdict


    def _gen_row_aslist_sql(self,columns):
        # get columnames
        db_cols_withid = self._get_columns_withid_sql()
        user_cols = {k:v for k,v in db_cols_withid.items() if k in columns} #refine columns
       
        cur = self.conn.cursor()
        sql = "SELECT idx, cells FROM {}".format(self.name)
        for row in cur.execute(sql):
            # debug cells_dict = json.loads(row["cells"])
            # log.debug(cells_dict)
            cells_dict = self._gen_cells_dict(row['cells'])
            row_aslist = []
            for col in columns:
                if user_cols[col] in cells_dict:
                    row_aslist.append(cells_dict[user_cols[col]])
                else:
                    row_aslist.append(None) #???? this is strange line of code. revisit!!
            yield row["idx"], row_aslist    
    
    
    def iterrows(self, 
                 columns: list=None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: bool=False):

        # validate user's args
        if columns is not None:
            ## need to work on validation from diff types of table
            db_columns = self._get_columns_sql()
            missing_cols = []
            for col in columns:
                if col not in db_columns:
                    missing_cols.append(col)
            if len(missing_cols) > 0:
                raise ValueError ('Error! Column {} not found.'.format(','.join(missing_cols)))
                

        if columns is None:
            columns = self.get_columns() # assign all available column names
            
        if row_type == 'list':
            for idx, row in self._gen_row_aslist_sql(columns):
                yield idx, row 
                    
        else: # assume row_type is dict 
            for idx, row in self._gen_row_asdict_sql(columns):
                yield idx, row 

    
    def _deprecated_update_row_sql(self, idx, column, value):
        """ using method that updates 'all' content of the cells"""
        row = self._get_row_sql(idx) # must get all columns to make cells complete for correct update to sql
                                    # will this be a trigger to change get_row's columns parameter???
        if row:                                    
            row[column] = value # get this row updated
            cursor = self.conn.cursor() 
            sql = f'UPDATE {self.name} SET cells = ? WHERE idx = ?;'
            db_cols_withid = self._get_columns_withid_sql()
            cells = json.dumps({db_cols_withid[k]: v for k,v in row.items()})
            params = [cells, idx]
            cursor.execute(sql, params)
        else:
            fn = inspect.stack()[0][3]
            warnings.warn(f'Warning! {fn}() trying to update a non existed row (ie. idx {idx}).')


    def _update_row_sql(self, idx, column, value):
        """ using json_set so only update for a specific key"""
        cursor = self.conn.cursor() 
        columnid = self._get_columnid_sql(column)
        sql_update = f"UPDATE {self.name} SET cells = (SELECT json_set({self.name}.cells, '$.{columnid}', ?)) where idx=?;"
        params = [value, idx]
        cursor.execute(sql_update, params)


    def _update_row_sql_bycolid(self, idx, columnid, value):
        """ using json_set so only update for a specific key and accept columnid
        Note: no performance increase even not calling get_columnid_sql()"""
        cursor = self.conn.cursor() 
        sql_update = f"UPDATE {self.name} SET cells = (SELECT json_set({self.name}.cells, '$.{columnid}', ?)) where idx=?;"
        params = [value, idx]
        cursor.execute(sql_update, params)    
                            

