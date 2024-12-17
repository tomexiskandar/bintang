from multiprocessing import Process, Manager, Pool
from itertools import repeat
import os
from datetime import datetime

def myf(myd):
    myd[1] = "Hello world!"

def proc(d):
    myf(d)

def info(t):
    # print(title)
    # print('module name:', __name__)
    # print(title,'parent process:', os.getppid())
    # print(title,'process id:', os.getpid())
    # d["proc"] = title
    # d["pid"] = os.getpid()
    _t = (t[0],t[1]["name"],os.getpid())
    return _t

# def f(name):
#     info('function f')
#     print('hello', name)
    

if __name__ == "__main__":
    manager = Manager()
    d = manager.dict()
    d = {"name":"John","city":"Umina"}

    # print('use map')
    # with Pool(processes=3) as pool:
    #     p = pool.starmap(info,zip(["one","two","three"],repeat(d)))
    # for res in p:
    #     print(res)
    
    print('use imap')
    with Pool(processes=3) as poolie:
        for res in poolie.imap(info,zip(["one","two","three"],repeat(d)),1):
            print(res)
