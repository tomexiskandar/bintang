import unicodedata
class Column:
    """class to define column object 
    """
    __slots__ = ('id','name','ordinal_position','data_type','column_size','decimal_digits','data_props')
    def __init__(self,name):
        self.id = None
        self.name = name
        self.ordinal_position = -1
        #self.data_types = [] # get this on request. its common data type more than one (inconsistent data)
        self.data_type = None
        self.column_size = 0 # the max. get this on request
        self.decimal_digits = 0 # get this on request
        self.data_props = {}
    
    def __repr__(self):
        return self.name

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