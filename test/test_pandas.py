import pandas as pd

if __name__ == '__main__':
    print('test')
    df = pd.read_json('menu.json') #music.json
    for index, row in df.iterrows():
       print(index,row)

    
