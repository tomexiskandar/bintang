import subprocess, os
from setuptools import sandbox
import time
from pathlib import Path
import shutil
from twine import *

# WARNING!!!!!!!
# BEFORE running this script, make sure:
# 1. increase version in \setup.py eg. x.x.x
# 2. log level on \bintang.log.py must be set to ERROR eg. log.setLevel(logging.ERROR)
# user name __token__
# password apitoken

if __name__ == "__main__":
    #sandbox.run_setup("setup.py",['sdist','--format=zip']) #python setup.py sdist
    # delete directory bingtang.egg-info, build and dist
    BASE_DIR = Path(__file__).parent
    print(BASE_DIR)
    build_dir = ['bintang.egg-info','build','dist']
    build_path = ['{}'.format(BASE_DIR / x) for x in build_dir]
    for path in build_path:
        try:
            shutil.rmtree(path)
        except:
            pass

    # build a wheel package
    sandbox.run_setup("setup.py",['bdist_wheel'])
    myenv = os.environ.copy()
    subprocess.run(["python","-m","twine","upload","dist/*"], env=myenv, shell=True)
    


