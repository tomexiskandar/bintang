class Column:
    """class to define column object 
    """
    def __init__(self,name):
        self.id = None
        self.name = name
        #self.data_types = [] # get this on request. its common data type more than one (inconsistent data)
        self.data_type = None
        self.column_size = 0 # the max. get this on request
        self.decimal_digits = 0 # get this on request
        self.data_props = {}
    
    # def __repr__(self):
    #     return {'columnname':self.name}

    def describe(self):
        print('{}(id:{}, name:{})'.format(__class__.__name__,self.id, self.name))

    def get_name_uppercased(self):
        return self.name.upper()