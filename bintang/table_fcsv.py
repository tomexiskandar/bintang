from bintang.table import Base_Table
from bintang.log import log

class From_CSV_Table(Base_Table):
    def __init__(self, name, filepath, bing=None, delimiter=',', quotechar='"', header_row=1):
        super().__init__(name, bing=bing)
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.header_row = header_row
        self.columns = self.get_columns()


    def get_columns(self):
        import csv
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            # determine columns
            columns = []
            for rownum, row in enumerate(reader, start=1):
                if rownum == self.header_row:
                    columns = [col for col in row] # add all columns
                    f.seek(0) # return to BOF
            return tuple(columns)
                    

    

    
    def iterrows(self, 
                 columns: list | tuple | None = None, 
                 row_type: str='dict', 
                 where=None, 
                 rowid: bool=False):

        import csv
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            next(reader)
            # iterate each line
            # for i in range(self.header_row):
            #     print('i', i)
            #     next(reader)
            for rownum, row in enumerate(reader, start=1):
                if len(self.columns) == len(row):
                    row_dict = dict(zip(self.columns, row))
                    if columns is not None:
                        row_dict = {col: row_dict[col] for col in columns}
                        if row_type == 'list':
                            yield rownum, [row_dict[col] for col in columns]
                        else:
                            yield rownum, row_dict
                    else:
                        if row_type == 'list':
                            yield rownum, row
                        else:
                            yield rownum, row_dict
                else:
                    raise IndexError ('length of column and row is not the same at rownum {}. Possible issues were incorrect quoting or missing value.'.format(rownum))


    def set_to_sql_colmap(self, columns):
        if isinstance(columns, list) or isinstance(columns, tuple):
            return dict(zip(columns, columns))
        elif isinstance(columns, dict):
            return columns

        
    def _get_sql_conn_name_xp(self, conn):
        # experimenting to get connector name
        # so code can act differently for eg. params list used in prepared statement.
        # at the moment only two db connectors being used, pyodbc and psycopg (specific to Postgresql)
        if str(type(conn)) == "<class 'pyodbc.Connection'>":
            return 'pyodbc'
        elif str(type(conn)) == "<class 'psycopg.Connection'>":
            return 'psycopg'
        else:
            raise ValueError('Sorry Only pyodbc and psycopg connection accepted!')  
    
    
    def to_sql(self, conn: object, 
               table: str, 
               columns: list | tuple | dict | None = None,
               schema: str=None,  
               method: str='prep', 
               max_rows: str = 1) -> int:
        """conn: accept only pyodbc.Connection or psycopg.Connection
           schema: database schema name
           table: table name in the database
           columns: If a dictionary then a columns mapping where the key is sql column (destination) and the value is bintang columns (source). 
                    If a list, column mapping will be created automatically assuming source columns and destination columns are the same. 
                    If not provided it assumes that user wants to insert all the columns from the table.
           method: prep=Prepared (default) or string
           return-> row_count
        """
        conn_name = self._get_sql_conn_name_xp(conn)
        if columns is None: # check if user want to include all columns in the table
            columns = self.get_columns()
        else:
            if isinstance(columns,list) or isinstance(columns, tuple):
                columns= self.validate_columns(columns)
            elif isinstance(columns,dict):
                columns_key = [x for x in columns.keys()]
                columns_val = [x for x in columns.values()] ## trouble maker
                columns_val= self.validate_columns(columns_val) ## trouble maker
                columns = dict(zip(columns_key, columns_val))
            else: 
                raise ValueError('Error! Only list/tuple or dict allowed for columns.')    
            
        if method == 'prep':
            return self._to_sql_prep(conn, table, columns, schema=schema, max_rows=max_rows,conn_name=conn_name)
        elif method =='string':
            return self._to_sql_string(conn, table, columns, schema=schema, max_rows=max_rows, conn_name=conn_name)

 
    def _to_sql_string(self, conn, table, columns, schema=None, max_rows = 300, conn_name='psycopg'):
        colmap = self.set_to_sql_colmap(columns)
        src_cols = [x for x in colmap.values()]
        dest_columns = [x for x in colmap.keys()]
        
        if schema:
            sql_template = 'INSERT INTO "{}"."{}" ({}) VALUES'
            str_stmt = sql_template.format(schema,table,",".join(['"{}"'.format(x) for x in colmap]))
        else:
            sql_template = 'INSERT INTO "{}" ({}) VALUES'
            str_stmt = sql_template.format(table,",".join(['"{}"'.format(x) for x in colmap]))
        
        sql_cols_withtype = self.set_sql_datatype(dest_columns, conn, schema, table)
        sql_cols_withliteral = None if sql_cols_withtype is None else self.set_sql_literal(sql_cols_withtype, conn)
        log.debug(sql_template)
        # start insert to sql
        cursor = conn.cursor()
        temp_rows = []  
        total_rowcount = 0 # to hold total record affected
        for idx, values in self.iterrows(src_cols, row_type='list'):
            ## question: can values and d_cols_withliteral align? dict python 3.7 is ordered and will solve it?
            sql_record = self.gen_sql_literal_record(values, sql_cols_withliteral)
            
            temp_rows.append(sql_record)
            if len(temp_rows) == max_rows:
                stmt = str_stmt + ' {}'.format(",".join(temp_rows))
                log.debug(stmt)
                cursor.execute(stmt)
                total_rowcount += cursor.rowcount
                temp_rows.clear()
        if len(temp_rows) > 0:
            stmt = str_stmt + ' {}'.format(",".join(temp_rows))
            log.debug(stmt)
            cursor.execute(stmt)
            total_rowcount += cursor.rowcount
        return total_rowcount           


    def gen_sql_literal_record(self, values, sql_cols_withliteral=None):
        sql_record = []
        if sql_cols_withliteral:
            sql_columns = [x for x in sql_cols_withliteral.keys()]
        for i, value in enumerate(values):
            if sql_cols_withliteral:
                col = sql_columns[i]
                literals = sql_cols_withliteral[col]
                sql_value = self.gen_sql_literal_value(value, literals)
            else:
                sql_value = self.gen_sql_literal_value(value)
            sql_record.append(sql_value)
        return "({})".format(','.join(sql_record))
        

    def gen_sql_literal_value(self, value, literals=None):
        if value == "" or value is None:
            return "NULL"
        if isinstance(value, str):
            value = value.replace("'","''")
        if literals:    
            return "{}{}{}".format('' if literals[0] is None else literals[0], value, '' if literals[1] is None else literals[1])
        else:
            if type(value) in [int, float, bool]:
                return str(value) # so we can just join them later
            else:
                return f"'{value}'"
            # return "{}{}{}".format('' if literals[0] is None else literals[0], value, '' if literals[1] is None else literals[1])
    

    def get_sql_typeinfo_table(self, conn):
        cursor = conn.cursor()
        sql_type_info_tuple = cursor.getTypeInfo(sqlType = None)
        columns_ = [column[0] for column in cursor.description]
        tobj = Memory_Table('sql_typeinfo')
        for row in sql_type_info_tuple:
            tobj.insert(row, columns_)
        return tobj
    

    def set_sql_datatype(self, dest_columns, conn, schema, table):
        cursor = conn.cursor()
        try:
            sql_columns = cursor.columns(schema=schema, table=table)
            columns_ = [column[0] for column in cursor.description]
            tobj = Memory_Table('sql_columns_')
            for row in sql_columns: #cursor.columns(schema=schema, table=table):
                tobj.insert(row, columns_)
            sql_columns_withtype = {}    
            for col in dest_columns:
                _type = tobj.get_value('type_name', where = lambda row: row['column_name']==col)
                sql_columns_withtype[col] = _type
            return sql_columns_withtype
        except: # oppss.. no support then fill up None
            return None  
    

    def set_sql_literal(self, sql_cols_withtype, conn):
        try:
            sql_typeinfo_tab = self.get_sql_typeinfo_table(conn)
            sql_cols_withliteral = {}
            for k, v in sql_cols_withtype.items():
                prefix = sql_typeinfo_tab.get_value('literal_prefix',where=lambda row: row['type_name']==v)
                suffix = sql_typeinfo_tab.get_value('literal_suffix',where=lambda row: row['type_name']==v)
                literals = (prefix,suffix)
                sql_cols_withliteral[k] = literals
            return sql_cols_withliteral    
        except: # opps... no support
            literals = (None, None)
            return {col:literals for col in sql_cols_withtype.keys()}
        


    def _to_sql_prep(self, conn, table, columns, schema=None, max_rows = 1, conn_name='pyodbc'):
        # if max_rows <= len(self): # validate max_row
        #     mrpb = max_rows # assign max row per batch
        # else:
            #log.warning('Warning! max_rows {} set greater than totalrows {}. max_rows set to 1'.format(max_rows, len(self)))
            # mrpb = 1
        mrpb = 1    
        numof_col = len(columns) # num of columns
        colmap = self.set_to_sql_colmap(columns)
        src_cols = [x for x in colmap.values()]
        dest_columns = [x for x in colmap.keys()]
        
        # create as prepared statement
        param_markers = self.gen_row_param_markers(numof_col, mrpb, conn_name=conn_name)
        if schema:
            sql_template = 'INSERT INTO "{}"."{}" ({}) VALUES {}'
            prep_stmt = sql_template.format(schema, table, ",".join(['"{}"'.format(x) for x in dest_columns]),param_markers)
        else:
            sql_template = 'INSERT INTO "{}" ({}) VALUES {}'
            prep_stmt = sql_template.format(table, ",".join(['"{}"'.format(x) for x in dest_columns]),param_markers)
        cursor = conn.cursor()
        temp_rows = []
        total_rowcount = 0
        for idx, row in self.iterrows(columns=src_cols, row_type='list'):
            for v in row:
                temp_rows.append(v)
            if len(temp_rows) == (mrpb * numof_col):
                log.debug(prep_stmt)
                log.debug(temp_rows)
                try:
                    cursor.execute(prep_stmt, temp_rows)
                except Exception as e:
                    log.error(e)
                    log.error(prep_stmt)
                    log.error(temp_rows)  

                total_rowcount += cursor.rowcount
                temp_rows.clear()     
        
        if len(temp_rows) > 0: # if any reminder
            param_markers = self.gen_row_param_markers(numof_col, int(len(temp_rows)/numof_col), conn_name=conn_name)
            prep_stmt = sql_template.format(schema,table,",".join(['"{}"'.format(x) for x in dest_columns]),param_markers)
            cursor.execute(prep_stmt, temp_rows)
            total_rowcount += cursor.rowcount
        return total_rowcount        


    def gen_row_param_markers(self,numof_col,num_row, conn_name='pyodbc'):
        p = "%s" if conn_name == 'psycopg' else "?"
        param = "(" + ",".join([p]  *numof_col) + ")"
        params = []
        for i in range(num_row):
            params.append(param)
        return ",".join(params)
    
    
