# other module import
# ...
import bintang

# example dict object
dict_obj = {
        'Page': 100,
        'Time': '2033-09-05T00:00:00Z',
        'Person': [
            {
                'id': 1,'name': 'John','surname': 'Smith',
                'Address': {'number': 1, 'street': 'Station','street_type': 'Street'}
            },
            {
                'id': 2,'name': 'Jane','surname': 'Brown',
                'Address': {'number': 8,'street': 'Parade','street_type': 'Road'}
            }
        ],
        'PersonDetails': [
            {
                'person_id': '1', 'hobby': 'Blogging','is_meat_eater': True
            },
            {
                'person_id': '2','hobby': 'Reading','is_meat_eater': None,
                'LuckyDays': [13,17,19]
            }
        ]
    }

bt = bintang.Bintang()
bt.read_dict(dict_obj)

# expect 5 tables generated:
expected_table = ['/', '//Person', '//Person/Address', '//PersonDetails', '//PersonDetails/LuckyDays']
tables = bt.get_tables()
assert set(tables) == set(expected_table)

# page column in root table must have value 100
res = bt['/'].get_value('Page')
assert res == 100

# Person table must have 2 rows
res = len(bt['//Person'])
assert res == 2

# person with id 1 must have name John
res = bt['//Person'].get_value(lambda row: row['name'], where=lambda row: row['id']==1)
assert res == 'John'

# '//PersonDetails/LuckyDays' table must have 3 rows
res = len(bt['//PersonDetails/LuckyDays'])
assert res == 3