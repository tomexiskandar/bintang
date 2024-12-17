import pyodbc
# import app_adhoc
import sys
import os
import time
import threading
from pathlib import Path
# import bintang
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0,os.path.join(PROJECT_ROOT,"bintang"))
import bintang
import logging
import yaml
from thefuzz import process as thefuzzprocess
import helper as h

if __name__ == '__main__':
     # # read data from sql server
    conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=EHL5CD8434KLM;PORT=1443;DATABASE=test;Trusted_Connection=yes;"
    conn = pyodbc.connect(conn_str) # the connection to the database
    # datasource paths
    path1 = r"C:\Users\60145210\Documents\Projects\bintang\test\specs.xlsx"
    #path2 = r"C:\Users\60145210\Documents\AFMO Data\20230316 MNCLHD CHBH Intercom\T\ref\cstTI - Manufacturer - Training - src_manuf out.xlsx"
    # read excel 4,653 rps
    # to_sql 840378/2m,21sec

    
    bt = bintang.Bintang()
    h.tic()
    #bt.read_excel(path1,'test')
    bt.create_table('test')
    bt['test'].read_excel(path1, 'specs')
    h.tac()
    # for idx, row in bt['test'].iterrows():
    #     print(idx, row)
    # if bt['test'].index_exists(27):
    #     print('exists')
    # quit()
    # for idx, row in bt['test'].iterrows():
    #     print(idx, row)
    print(bt['test'].get_columns())

    res = bt['test'].groupby(['Manufacturer Name'], 'output',
                                 group_count=True, 
                                 #counts=['Manufacturer Name','Model Name'],
                                 counts=[('Manufacturer Name','countOfManuf'),('Model Name','countOfModelName')],
                                 sums=[('Day','sumOfDay')])
    
    print('----')
    manuf = 0
    group_count = 0
    model_name = 0
    day_sum = 0

    for idx, row in bt['output'].iterrows(row_type='dict'):
        print(idx, row)
        # group_count += row['group_count']
        # manuf = manuf + row['count_Manufacturer Name']
        # model_name += row['count_Model Name']
        # day_sum += row['sum_Day']
        
        
        
        


    # print(manuf)
    # print(group_count)
    # print(model_name)
    # print(day_sum)
    path = Path(__file__).resolve().parent / 'dev2_specs_groupby.xlsx'
    bt['output'].to_excel(path)
        


    