import pandas as pd
import helper as h

if __name__ == '__main__':
    path1 = r"C:\Users\60145210\Documents\AFMO Data\20230316 MNCLHD CHBH Intercom\T\ref\cstTI - Specs - BF - AFMO - Training - src_all_spec out.xlsx"
    h.tic()
    df = pd.read_excel(path1,sheet_name='Sheet1')
    h.tac()
    h.tic()
    for idx,row in df.iterrows():
        pass
    h.tac()
    

    
