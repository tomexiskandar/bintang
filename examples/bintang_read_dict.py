# other module import
# ...
import bintang

# example dict object
dict_obj = {
     'Page': 100,
     'Time': '2033-09-05T00:00:00Z',
     'Person': [
         {'id': 1,'name': 'John','surname': 'Smith',
             'Address': {'number': 1, 'street': 'Station','street_type': 'Street'}
         },
         {'id': 2,'name': 'Jane','surname': 'Brown',
             'Address': {'number': 8,'street': 'Parade','street_type': 'Road'}
         }
     ],
     'PersonDetails': [
         {'person_id': '1', 'hobby': 'Blogging','is_meat_eater': True
         },
         {'person_id': '2','hobby': 'Reading','is_meat_eater': None,
             'LuckyDays': [13,17,19]
         }
     ]
}
bt = bintang.Bintang()
bt.read_dict(dict_obj)

print(bt) # show bt tables
# {
#    "name": null,
#    "tables": [
#       "/",
#       "//Person",
#       "//Person/Address",
#       "//PersonDetails",
#       "//PersonDetails/LuckyDays"
#    ]
# }

# loop through root table ('/')
for idx, row in bt['/'].iterrows():
    print(idx, row)
# 0 {'Page:': 100, 'Time': '2033-09-05T00:00:00Z'}

# loop through  //Person table.
for idx, row in bt['//Person'].iterrows():
    print(idx, row)
# 0 {'Person': 0, 'id': 1, 'name': 'John', 'surname': 'Smith'}
# 1 {'Person': 1, 'id': 2, 'name': 'Jane', 'surname': 'Brown'}

# print //Person/Address table. Because this table under //Person, then each record will have their own
# reference to //Person table.

bt['//Person/Address'].print()

#                      Table: //Person/Address
# -----------+--------------+--------------+-----------+---------------
#   Address  |    Person    |    number    |   street  |  street_type
# -----------+--------------+--------------+-----------+---------------
#  Address   |            0 |            1 | Station   | Street
#  Address   |            1 |            8 | Parade    | Road
# -----------+--------------+--------------+-----------+---------------
# (2 rows)

assert len(bt['//Person/Address']) == 2