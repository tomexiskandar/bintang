
import os
import sys
from pathlib import Path
import json

    
import bintang

if __name__ == '__main__':
    BASE_DIRPATH = Path(__file__).parent.parent
    
    data_dict = json.load(open(BASE_DIRPATH / 'bintang' / 'sqlcfg.json'))
    bt = bintang.Bintang("mybt")
    # tablepaths = ['/balances/special']
    bt.read_dict(data_dict)
    print(bt)
    bt['/postgresql/type_mappings'].name = 'tab'
    bt['tab'].print()

    bt['/postgresql/delimited_identifiers'].print()
    bt['/postgresql/delimited_identifiersx'].print()
    quit()
    val = bt['/postgresql/delimited_identifiersx'].get_value('start')
    print(val)
    quit()
    bt['/postgresql/type_info'].print()
    quit()
    
    # bt['tab'].groupby(['py'], 'grouped',group_count=True)
    
    # bt['grouped'].print()
    # tobj = bt['grouped'].filter(lambda row: row['group_count']>1)
    # tobj.print()
    # quit()

    bt['tab'].print()
    bt['tab'].set_index('py')
    bt['tab'].copy_index(at_start=True)
    bt['tab'].print()
    print(bt['tab'].name)
    print(bt['tab'].get_row('bites'))
    
    # print(bt)
    # print('*'*5)
    # for idx, row in bt['/balances/special'].iterrows():
    #     print(idx, row)
