import datetime
from bintang import Bintang
bt = Bintang()
bt.create_table('product')
p = bt['product']
p.add_column('id', data_type='int', min_value=1, required=True)
p.add_column('name', data_type='str', min_length=0, max_length=20, empty_string_as_null=True)
p.add_column('price', data_type='float', min_value=0)
p.add_column('order_date', data_type='date', max_value=datetime.date(2025,12,31), empty_string_as_null=True)
p.add_column('timestamp', data_type='datetime', empty_string_as_null=False)

p.insert({'id':1.0, 'name':'','price':'1.60', 'order_date':'2025-07-01'})
p.insert({'id':'', 'name':'Sinker','price':1.20}) 
p.insert({'id':'3', 'name':'Reels  ','price':15.50}) 
p.insert({'id':4.0, 'name':None, 'price':20, 'order_date': '2026-07-01'})
p.insert({'id':'5t', 'name':'Bait','price':20})
p.insert({'id':6, 'name':'Bait has very long name','price':5.50})


valid_rows = []
bad_rows = []
for idx, results in p.valrows(empty_string_as_null=True):
    if all([x[2] for x in results]): # check if all columns are valid:
        # do something with valid rows, for example, print a message
        print(f'valid row: idx:{idx} results: {results}')
        valid_rows.append(idx)
    else:
        print(f'bad row: idx:{idx} results: {results}')
        bad_rows.append(idx)

assert valid_rows == [1, 3]
assert bad_rows == [2, 4, 5, 6]

## the printed validation results
#valid row: idx:1 results: ((1, 1, True, None), (2, None, True, None), (3, 1.6, True, None), (4, datetime.date(2025, 7, 1), True, None), (4, datetime.date(2025, 7, 1), True, None))
#bad row: idx:2 results: ((1, None, False, 'column is required but value is None'), (2, 'Sinker', True, None), (3, 1.2, True, None), (3, 1.2, True, None), (3, 1.2, True, None))
#valid row: idx:3 results: ((1, 3, True, None), (2, 'Reels', True, None), (3, 15.5, True, None), (3, 15.5, True, None), (3, 15.5, True, None))
#bad row: idx:4 results: ((1, 4, True, None), (1, 4, True, None), (3, 20, True, None), (4, datetime.date(2026, 7, 1), False, 'value is above the maximum allowed value 2025-12-31'), (4, datetime.date(2026, 7, 1), False, 'value is above the maximum allowed value 2025-12-31'))
#bad row: idx:5 results: ((1, '5t', False, 'ValueError: Expected int, got string that cannot be converted to integer'), (2, 'Bait', True, None), (3, 20, True, None), (3, 20, True, None), (3, 20, True, None))
#bad row: idx:6 results: ((1, 6, True, None), (2, 'Bait has very long name', False, 'value has length 23 which is out of allowed range [0, 20]'), (3, 5.5, True, None), (3, 5.5, True, None), (3, 5.5, True, None))