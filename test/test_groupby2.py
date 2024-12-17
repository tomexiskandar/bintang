import os
import sys
from pathlib import Path
import json
import pyodbc


import bintang

class Person():
    def __init__(self):
        self.x = 'x'
        self.b = 1
    # def __ref__(self):
    #     return type(self).__name__
    
    # def __str__(self):
    #     return type(self).__name__ + ' obj'
    

       
if __name__ == "__main__":
    bt = bintang.Bintang("my bintang")
    p = Person()
    bt.create_table("Product")
    bt.create_table("Productt")
    p = bt['Product']
    p.add_column("added column")
    p.add_column("added column")

    p.insert({'id':1, 'brand': 'Shimano', 'class':'rod', 'name':'Extraction','price':299})
    p.insert({'id':2, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
    p.insert({'id':3, 'brand': 'Ugly Stik1234567', 'class':'rod', 'name':'Balance II','price':63.99})
    p.insert({'id':4, 'brand': 'Shimano', 'class':'rod', 'name':'Zodias Travel','price':399})
    p.insert({'id':5, 'brand': 'Shimano', 'class':'reel', 'sub class': 'spinning', 'name':'Sedona F1','price':99.00})
    p.insert({'id':6, 'brand': 'Shimano', 'class':'reel', 'sub class':'spinning', 'name':'FX Series 4000','price':54.99})
    # p.insert({'id':7, 'brand': 'Abu Garcia', 'class':'rod', 'name':'Veritas 4.0','price':123.35})
    # p.insert({'id':8, 'brand': 'Abu Garcia', 'class':'rod', 'name':'Veritas 4.0','price':123.35})
    # p.insert({'id':9, 'brand': 'Ugly Stik', 'class':'rod', 'name':'Gold II','price':87.99})
    # p.insert({'id':10, 'brand': 'Daiwa', 'class':'reel', 'sub class':'spinning', 'name':'Exceler LT','price':80.00})
    # p.insert({'id':11, 'brand': 'Daiwa', 'class':'reel', 'sub class':'spinning', 'name':'Crossfire','price':54.00})
    
    # for k, v in p._Table__columns.items():
    #     print(k, v.name, v.ordinal_position)
       
    # p.order_columns(['brand','price'])
    # # for idx, row in p._Table__rows.items():
    # #     print(idx, row)
    # columns = p.get_columns()
    # print(columns)
    

    # p.copy_index(column='index', at_start=True)
    

    # print()
    #bt['Product'].print(show_data_type=False)
    
    # for idx, row in p.iterrows():
    #     print(idx, row)
    
    grouped = p.groupby(['brand','class']  
                          ,group_count=True
                          #mins=['price'],
                          #maxs=['price']
                          #counts = [('price','price (count)')],
                          #sums = ['price'],
                          #sums = [('price','price (sum)')]
                          #means=['price']
                          #means=[('price','price (mean)')]
                          )
    bt.add_table(grouped)
    # for idx, row in bt['grouped_product'].iterrows():
    #     print(idx, row)

    #print(bt['grouped_product'])
    # print()
    # #bt['grouped_product'].reindex()
    # bt['grouped_product'].copy_index()
    grouped.print()
    # for idx, row in bt['grouped_product'].iterrows(where=lambda row: row['brand']=='Shimano'):
    #     print(idx, row)
    grouped.name= 'GroupConcat'


    grouped = p.groupby(['brand']
                          ,group_concat='id'  
                          ,sums=['price']
                          )
    grouped.print(square=True)

    bt.add_table(grouped)
    
    bt['grouped'].name = 'mygrouped'
    bt['mygrouped'].print(square=False)

    # print(bt)

    # bt.drop_table('mygrouped')

    # print(bt)






