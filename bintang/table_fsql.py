from bintang.table import Base_Table
import json

class From_SQL_Table(Base_Table):
    def __init__(self, name, bing=None):
        super().__init__(name, bing=bing)
        self.conn = None    # for fromsql use
        self.sql_str = None     # for fromsql use
        self.params = None  # for fromsql use      


    def __repr__(self):
        tbl = {}
        tbl['name'] = self.name
        columns = self.get_columns()
        tbl['columns'] = columns
        return json.dumps(tbl, indent=2) 
    
    
    def get_columns(self) -> tuple:
        cursor = self.conn.cursor()
        if self.sql_str is None:
            sql_str = "SELECT * FROM {}".format(self.name)
        if self.params is not None:
            cursor.execute(self.sql_str, self.params)
        else:
            cursor.execute(self.sql_str)
        return tuple([col[0] for col in cursor.description])


    def iterrows(self, 
                 row_type: str='dict', 
                 rowid: bool=False):
        """ Iterate through rows of the table.
        Args:
            row_type (str): 'dict' or 'list'. Default is 'dict'.
            rowid (bool): If True, will return row index as first element in the row. Default is False.
        Yields:
            tuple: (row_index, row_data) where row_data is a dict or list depending on row_type.
        Notes on implementation:
            - no Arg columns and values, as this is a linked table. pass any desired columns at sql str
            - no Arg where, as this is a linked table. pass any desired where clause at sql str
        """

        
        # define columns          
        columns = self.get_columns() # assign all available column names
            
        # execute sql
        cursor = self.conn.cursor()
        if self.sql_str is None:
            sql_str = "SELECT * FROM {}".format(self.name)
        if self.params is not None:
            cursor.execute(self.sql_str, self.params)
        else:
            cursor.execute(self.sql_str)
        columns_fromsql = [col[0] for col in cursor.description]
        ## use fetchmany instead of fetchone for better performance
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
            if row_type.upper() == 'LIST':
                for row in rows:
                    yield idx, [dict(zip(columns_fromsql, row))[x] for x in columns_fromsql] 
            else:
                for row in rows:
                    yield idx, {k: dict(zip(columns_fromsql, row))[k] for k in columns_fromsql}
            idx += 1


    def to_csv(self, path, index=False, \
               dialect='excel', delimiter=',', \
               quotechar='"', quoting=0): 
        import csv
        # csv.QUOTE_MINIMAL = 0
        # csv.QUOTE_ALL = 1
        # csv.QUOTE_NONNUMERIC = 2
        # csv.QUOTE_NONE = 3
        
        # define columns
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
                for idx, row in self.iterrows(row_type='list'):
                    csvwriter.writerow([idx] + row)
            else:
                for idx, row in self.iterrows(row_type='list'):
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