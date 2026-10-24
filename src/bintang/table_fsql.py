from bintang.log import log
from bintang.table_base import Base_Table
from bintang.cell import Cell
from bintang import quote_id
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


    def __len__(self):
        return -1


    def get_columnids(self, columns):
        pass

    def populate_columns(self):
        columns = self.get_columns_fromdb()
        for column in columns: # do it for Base_Table's __columns
            columnid = self.get_columnid(column)
            if columnid is None: # if columnid is None then assume user wants a new column
                self.add_column(column)
    
    
    def get_columns_fromdb(self) -> tuple:
        cursor = self.conn.cursor()
        try:
            if self.sql_str is None:
                sql_str = "SELECT * FROM {} WHERE 1=0".format(quote_id(self.name))
                cursor.execute(sql_str)           
            else:
                cursor.execute(self.sql_str)
            # Loop through the result sets to skip the setup steps (CREATE, INDEX etc) and reach the final SELECT result set
            # cursor.description is only populated when a set contains actual rows to fetch    
            while cursor.description is None:
                if not cursor.nextset():
                    break
            # Once the loop exits and finds cursor.description, fetch final data    
            if cursor.description is not None:    
                return tuple([col[0] for col in cursor.description])
        except Exception as e:
            log.exception('Error executing SQL: {}'.format(e))
        finally:
            cursor.close()   


    def _iterrows(self, rowid: str=None):
        # execute sql
        cursor = self.conn.cursor()
        try:
            if self.sql_str is None:
                self.sql_str = "SELECT * FROM {}".format(self.name)
            if self.params is not None:
                cursor.execute(self.sql_str, self.params)
            else:
                cursor.execute(self.sql_str)
            # Loop through the result sets to skip the setup steps (CREATE, INDEX etc) and get to the actual data result set.
            # cursor.description is only populated when a set contains actual rows to fetch    
            while cursor.description is None:
                if not cursor.nextset():
                    break    
            # Once the loop exits and finds cursor.description, fetch final data    
            if cursor.description is not None:
                ## use fetchmany instead of fetchone for better performance
                # row = cursor.fetchone()
                # idx = 1
                # while row is not None:
                #     yield idx, row
                #     row = cursor.fetchone()
                #     idx += 1 
                #
                columns_from_db_= tuple([col[0] for col in cursor.description])
                # generate local columns based on the columns from the database
                for column in columns_from_db_: 
                    columnid = self.get_columnid(column)
                    if columnid is None: # if columnid is None then assume user wants a new column
                        self.add_column(column)
                        
                columns_dict = {} # to hold column id to column name mapping
                for column in self.get_columns():
                    cobj = self.get_column_object(column)
                    columns_dict[cobj.id] = column 
                idx = 1
                while True:
                    rows = cursor.fetchmany(300)
                    if not rows: break
                    if rowid is not None:
                        for sqlrow in rows:
                            row_dict = dict(zip(columns_dict.keys(), sqlrow))
                            row = self.make_row()
                            try: # user may not have rowid passed to the query but still passed rowid arg
                                rowid_value = row_dict[self.get_columnid(rowid)]
                            except KeyError:
                                raise KeyError(f"Row ID column '{rowid}' not found in SQL row.")
                            for k, v in row_dict.items():
                                cell = Cell(k, v)
                                row.add_cell(cell)
                            yield rowid_value, row
                            idx += 1
                    else:
                        for sqlrow in rows:
                            # print('sqlrow',sqlrow)
                            row_dict = dict(zip(columns_dict.keys(), sqlrow))
                            row = self.make_row()
                            for k, v in row_dict.items():
                                cell = Cell(k, v)
                                row.add_cell(cell)
                            yield idx, row
                            idx += 1
        except Exception as e:
            log.exception('Error executing SQL: {}'.format(e))
        finally:
            cursor.close()               

    
    
    
    def iterrows(self,
                    columns: list | tuple | None = None,
                    row_type: str='dict', 
                    rowid: str=None):
        """ Iterate through rows of the table.
        Args:
            columns: select only specific columns during iteration. Default is None which is to select all columns.
            row_type (str): 'dict' or 'list'. Default is 'dict'.
            rowid (str): a column in the data source that will be used as index. If not an automatic index will be generated.
        Yields:
            tuple: (row_index, row_data) where row_data is a dict or list depending on row_type.
        Notes on implementation:
            - no Arg columns and values, as this is a linked table. pass any desired columns at sql str
            - no Arg where, as this is a linked table. pass any desired where clause at sql str
        """

        
        # define columns
        if columns is None:          
            columns = self.get_columns() # assign all available column names

            
        # execute sql
        cursor = self.conn.cursor()
        try:
            if self.sql_str is None:
                sql_str = "SELECT * FROM {}".format(self.name)
            if self.params is not None:
                cursor.execute(self.sql_str, self.params)
            else:
                cursor.execute(self.sql_str)
            # Loop through the result sets to skip the setup steps (CREATE, INDEX etc) and get to the actual data result set.
            # cursor.description is only populated when a set contains actual rows to fetch    
            while cursor.description is None:
                if not cursor.nextset():
                    break    
            # Once the loop exits and finds cursor.description, fetch final data    
            if cursor.description is not None:
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
                    if rowid is not None:
                        if row_type.upper() == 'LIST':
                            for row in rows:
                                row_dict = {k: dict(zip(columns_fromsql, row))[k] for k in columns_fromsql}
                                yield row_dict[rowid], row_dict  
                                idx += 1
                        else:
                            for row in rows:
                                row_dict = {k: dict(zip(columns_fromsql, row))[k] for k in columns_fromsql}
                                yield row_dict[rowid], row_dict 
                                idx += 1
                    else:
                        if row_type.upper() == 'LIST':
                            for row in rows:
                                yield idx, [dict(zip(columns_fromsql, row))[x] for x in columns_fromsql] 
                                idx += 1
                        else:
                            for row in rows:
                                yield idx, {k: dict(zip(columns_fromsql, row))[k] for k in columns_fromsql}
                                idx += 1
        except Exception as e:
            log.error('Error executing SQL: {}'.format(e))
        finally:
            cursor.close()               
            


    # to_csv() move to table_base
    

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