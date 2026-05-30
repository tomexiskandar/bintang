# other module import
# ...
import bintang

# example dict object
dict_obj =  [
         {
             'id': 1,'name': 'John','surname': 'Smith'
         },
         {
             'id': 2,'name': 'Jane','surname': 'Brown'
         }
     ]
     
bt = bintang.Bintang()
bt.read_dict(dict_obj)

# '/' (the root table) has two rows 
assert len(bt['/']) == 2
# and three columns (/ ,id, name, surname)
assert bt['/'].get_columns() == ('/', 'id', 'name', 'surname')