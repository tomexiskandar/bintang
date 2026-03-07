import unicodedata
class Column:
    """class to define column object 
    """
    __slots__ = ('id','name','data_type','required','min_value','max_value','min_length','max_length','ordinal_position','column_size','decimal_digits','data_props')
    def __init__(self,name):
        self.id = None
        self.name = name
        self.data_type = None  # for validation
        self.required = False # for validation
        self.min_value = None # for validation for int and float
        self.max_value = None # for validation for int and float
        self.min_length = None # for validation for str
        self.max_length = None # for validation for str
        self.ordinal_position = -1
        self.column_size = 0 # the max. get this on request
        self.decimal_digits = 0 # get this on request
        self.data_props = {}
        #self.data_types = [] # get this on request. its common data type more than one (inconsistent data)
        
        
    
    def __repr__(self):
        return f'id:{self.id}, name: {self.name}, data_type: {self.data_type}, required: {self.required}, min_value: {self.min_value}, max_value: {self.max_value}, min_length: {self.min_length}, max_length: {self.max_length}'

    def get_name_uppercased(self):
        return self.name.upper()
    
    # def get_name_normalize_caseless(self):
    #     return unicodedata.normalize("NFKD", self.name.casefold())
    
    def get_greatest_column_size_data_prop(self):
        """data props can hold diffrent data type
        depending data source provided."""
        column_size = 0
        data_prop = None
        for k, v in self.data_props.items():
            if v['column_size'] > column_size:
                 column_size = v['column_size']
                 data_prop = {k: v}
        return data_prop
    
    def has_string_data_type(self):
        #print('hello')
        for dt in self.data_props.keys():
            #print('  dt',dt)
            if dt == 'str':
                return True
        return False


    def get_data_types(self):
        # data type from populated data
        data_types = []
        for dt in self.data_props.keys():
            data_types.append(dt)
        return data_types