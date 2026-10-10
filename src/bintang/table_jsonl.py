from bintang.log import log
from bintang.table_base import Base_Table
import json

class From_JSONL_Table(Base_Table):
    def __init__(self, name, filepath, bing=None):
        super().__init__(name, bing=bing)
        self.filepath = filepath


    def __len__(self):
        """ return the length of rows"""
        with open(self.filepath, "rb") as file:
            # Read the file in large binary chunks to save memory and maximize speed
            return sum(chunk.count(b"\n") for chunk in iter(lambda: file.read(1024 * 1024), b""))



    def get_columnid(self):
        pass


    def get_columnids(self):
        pass

    def get_columns(self):
            with open(self.filepath, 'r', encoding='utf-8') as f:
                for line in f:
                    row_dict = json.loads(line)
                    return tuple([x for x in row_dict.keys()])


    def iterrows(self, 
                     columns: list | tuple | None = None, 
                     row_type: str='dict', 
                     where=None, 
                     rowid: bool=False):
            with open(self.filepath, 'r', encoding='utf-8') as f:
                if columns is None:
                    for idx, line in enumerate(f, start=1):
                        # 1. Parse the JSON string into a Python dictionary
                        row_dict = json.loads(line)
                        if row_type == 'list':
                            yield idx, [x for x in row_dict.values()]
                        else:
                            yield idx, row_dict
                        
                else:
                    for idx, line in enumerate(f, start=1):
                        row_dict = json.loads(line)
                        if row_type == 'list':
                            yield idx, [row_dict[key] for key in columns]
                        else:
                            print('hello')
                            yield idx, {key: row_dict[key] for key in columns}
                        
