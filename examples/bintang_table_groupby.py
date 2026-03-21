import bintang
bt = bintang.Bintang()
bt.create_table('Product') # this will be our basis table for grouping
p = bt['Product'] # assign p as the table
p.insert({'id':1, 'brand': 'Shimano', 'class':'rod', 'name':'Extraction','price':299})
p.insert({'id':2, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
p.insert({'id':3, 'brand': 'Ugly Stik', 'class':'rod', 'name':'Balance II','price':63.99})
p.insert({'id':4, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
p.insert({'id':5, 'brand': 'Shimano', 'class':'reel', 'sub class': 'spinning', 'name':'Sedona F1','price':99.00})
p.insert({'id':6, 'brand': 'Shimano', 'class':'reel', 'sub class':'spinning', 'name':'FX Series 4000','price':54.99})

grouped = p.groupby(['brand', 'class'], group_count=True)

grouped.print()
#              Table: grouped
# -------------+---------+---------------
#     brand    |  class  |  group_count
# -------------+---------+---------------
#  Shimano     | rod     |             3
#  Ugly Stik   | rod     |             1
#  Shimano     | reel    |             2
# -------------+---------+---------------
# (3 rows)

grouped = p.groupby(['brand'], group_concat='id', sums=['price']) # another example

grouped.print()
#                     Table: grouped
# -------------+-------------------+-------------------
#     brand    |    group_concat   |     sum_price
# -------------+-------------------+-------------------
#  Shimano     |   [1, 2, 4, 5, 6] |           1250.99
#  Ugly Stik   |               [3] |             63.99
# -------------+-------------------+-------------------
# (2 rows)