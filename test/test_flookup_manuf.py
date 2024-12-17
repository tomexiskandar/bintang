from bintang import Bintang
import time


if __name__ == '__main__':
    bt = Bintang()
    manuf_trg_path = r"C:\Users\60145210\Documents\Projects\bintang\test\manuf\AFMO Upload Template NSLHD 2a - RNS - SC - Release 3.0 1 cleansed.xlsx"
    bt.create_table('manuf_trg')
    bt['manuf_trg'].read_excel(manuf_trg_path, 'Sheet3')

    #bt['manuf_trg'].print()

    manuf_src_path = r"C:\Users\60145210\Documents\Projects\bintang\test\manuf\cstTI - Manufacturer - src_manuf out.xlsx"
    bt.create_table('manuf_src')
    bt['manuf_src'].read_excel(manuf_src_path, 'Sheet1')

    start = time.time()
    res = bt['manuf_trg'].flookup('manuf_src',
                            [('Manufacturer Name','Name')],
                            ['Name'],
                            min_ratio = 70,
                            ret_matches = True)

    lapsed_time = time.time() - start
    print('lapsed time:', lapsed_time)
    # delete records that has rank greater than 5
    res.delete(lambda x: x['rank'] > 2)
    
                            
    # lookup Manufacturer Name
    bt['manuf_trg'].copy_index() # need idx to be copied first
    res.blookup(bt['manuf_trg']
                ,[('lidx','idx')]
                ,['Manufacturer Name'])

    # lookup src Manufacturer
    bt['manuf_src'].copy_index() # need idx to be copied first
    res.blookup(bt['manuf_src']
                ,[('ridx','idxx')]
                ,['Name'])          

    res.print(show_data_type=True)

