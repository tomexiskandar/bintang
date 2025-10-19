from bintang.table import Base_Table
import json

class From_SQL_Table(Base_Table):
    def __init__(self, name, bing=None):
        super().__init__(name, bing=bing)
        self.fromsql_conn = None    # for fromsql use
        self.fromsql_str = None     # for fromsql use
        self.fromsql_params = None  # for fromsql use      


    def __repr__(self):
        tbl = {}
        tbl['name'] = self.name
        columns = self.get_columns()
        tbl['columns'] = columns
        return json.dumps(tbl, indent=2) 
    
    
    def _get_columns_fromsql(self) -> list:
        cursor = self.fromsql_conn.cursor()
        if self.fromsql_str is None:
            sql_str = "SELECT * FROM {}".format(self.name)
        if self.fromsql_params is not None:
            cursor.execute(self.fromsql_str, self.fromsql_params)
        else:
            cursor.execute(self.fromsql_str)
        return [col[0] for col in cursor.description]


    def get_columns(self) -> tuple:
        sorted_columns = self._get_columns_fromsql()
        return tuple(sorted_columns)


    def _iterrows_fromsql(self, 
                 columns: list=None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: bool=False):
        
        # validate user's args
        if columns is not None:
            pass
                  
        if columns is None:
            columns = self.get_columns() # assign all available column names
            
        if row_type == 'list':
            # execute sql
            cursor = self.fromsql_conn.cursor()
            if self.fromsql_str is None:
                sql_str = "SELECT * FROM {}".format(self.name)
            if self.fromsql_params is not None:
                cursor.execute(self.fromsql_str, self.fromsql_params)
            else:
                cursor.execute(self.fromsql_str)
            columns_fromsql = [col[0] for col in cursor.description]
            # row = cursor.fetchone()
            # idx = 1
            # while row is not None:
            #     yield idx, row
            #     row = cursor.fetchone()
            #     idx += 1 
            # 
            idx = 1
            while True:
                rows = cursor.fetchmany(300)
                if not rows: break
                if columns is not None:
                    for row in rows:
                        yield idx, [dict(zip(columns_fromsql, row))[x] for x in columns_fromsql] 
                        idx += 1
                else:
                    for row in rows:
                        yield idx, list(row)
                        idx += 1    
        else:  # assume row_type as dict
            # execute sql
            cursor = self.fromsql_conn.cursor()
            if self.fromsql_str is None:
                sql_str = "SELECT * FROM {}".format(self.name)
            if self.fromsql_params is not None:
                cursor.execute(self.fromsql_str, self.fromsql_params)
            else:
                cursor.execute(self.fromsql_str)
            columns_fromsql = [col[0] for col in cursor.description]
            idx = 1 # initial index
            while True:
                rows = cursor.fetchmany(300)
                if not rows: break
                if columns_fromsql is not None:
                    for row in rows:
                        yield idx, {k: dict(zip(columns_fromsql, row))[k] for k in columns_fromsql}
                        idx += 1
                else:
                    for row in rows:
                        yield idx, dict(zip(columns_fromsql, row))
                        idx += 1            

        
    def iterrows(self, 
                 columns: list=None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: bool=False):
        
        for idx, row in self._iterrows_fromsql(columns, row_type, where, rowid):
            yield idx, row


    def to_csv(self, path, columns=None, index=False, \
               dialect='excel', delimiter=',', \
               quotechar='"', quoting=0): 
        import csv
        # csv.QUOTE_MINIMAL = 0
        # csv.QUOTE_ALL = 1
        # csv.QUOTE_NONNUMERIC = 2
        # csv.QUOTE_NONE = 3
        
        if columns is None:
            columns = self.get_columns()
        with open(path, 'w', newline = '\n') as csvfile:
            csvwriter = csv.writer(csvfile, dialect=dialect, delimiter=delimiter,
                                   quotechar=quotechar, quoting=quoting)
            columns_towrite = [col for col in columns]
            if index:                       # if column index wanted
                idx_col = INDEX_COLUMN_NAME
                if isinstance(index, str):  # if user wanted own index column name
                    idx_col = index
                columns_towrite.insert(0,idx_col)
            # write header to csvfile
            csvwriter.writerow(columns_towrite)
            if index:
                for idx, row in self.iterrows(columns, row_type='list'):
                    csvwriter.writerow([idx] + row)
            else:
                for idx, row in self.iterrows(columns, row_type='list'):
                    csvwriter.writerow(row)
    

    


type_map = {
    'sqlserver': {
        'type_mappings': {
            'str':'nvarchar'
            ,'int':'int'
            ,'datetime':'datetime'
            ,'float':'float'
            ,'bool':'bit'
            ,'bytes':'varbinary'
        },
        'delimited_identifiers':{'start':'[]', 'end':']'},
        'type_info': {
        'varchar': {'literal_prefix':"'", 'literal_suffix':"'"}
        } 
    },
    'postgresql': {
        'type_mappings': {
            'str':'varchar'
            ,'int':'INTEGER'
            ,'datetime':'timestamp'
            ,'float':'float8'
            ,'bool':'boolean'
            ,'bytes':'bytes'
        },
        'delimited_identifiers':{'start':'"', 'end':'"'},
        'type_info': {
            'varchar': {'literal_prefix':"'", 'literal_suffix':"'"}
        }   
    }            
}        