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
import helper as h
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='test_groupby_excel.py'
                                    ,usage='%(prog)s path [options]'
                                    ,description='Read excel file and do group function.'
                                    ,epilog='eg. %(prog)s "C:\\Users\\Monty\\Projects\\MyProject\myexcel.xlsx"')
    parser.add_argument('path', type=str, help='The path of the excel file')
    parser.add_argument('--sheetname', type=str, dest= 'sheetname', default='Sheet1', help="The worksheet name. If not provided then Sheet1 will be used.")
    parser.add_argument('--columns', type=str, dest= 'columns', help="a comma separated list of columns, for eg. 'cola, colb'")
    args = parser.parse_args()
    bt = bintang.Bintang()
    bt.create_table(args.sheetname)
    bt[args.sheetname].read_excel(args.path, args.sheetname)

    # print(type(args.columns))
    # print(args.columns)
    columns = args.columns.split(",")
    columns = [col.strip() for col in columns]
    # print(columns)
  
    # print(type(columns))

    res = bt[args.sheetname].groupby(columns, group_count=True, drop_none=False)
  
    # for idx, row in res.iterrows():
    #     print(idx, row)

    res.print()    
    
    

    