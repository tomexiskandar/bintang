from bintang.table import Base_Table
from bintang.log import log
import csv

class From_CSV_Table(Base_Table):
    def __init__(self, name, filepath, bing=None, delimiter=',', quotechar='"', header_row=1):
        super().__init__(name, bing=bing)
        self.filepath = filepath
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.header_row = header_row
        self.columns = self.get_columns()


    def __len__(self):
        """ return the length of rows"""
        with open(self.filepath, newline='') as f:
            reader = csv.reader(f, delimiter=self.delimiter, quotechar=self.quotechar)
            row_count = len(list(reader)) -1
        return row_count


    def get_columns(self):
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
            