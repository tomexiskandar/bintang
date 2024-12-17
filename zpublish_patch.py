import subprocess, os
from setuptools import sandbox
import time
from pathlib import Path
import shutil
from twine import *
import yaml
import requests
import json
from bintang import Bintang
from setuptools import setup
try:
    from packaging.version import parse
except ImportError:
    from pip._vendor.packaging.version import parse

URL_PATTERN = 'https://pypi.python.org/pypi/{package}/json'
BASE_DIR = Path(__file__).parent


def get_version(package, url_pattern=URL_PATTERN):
    """Return version of package on pypi.python.org using json."""
    req = requests.get(url_pattern.format(package=package))
    version = parse('0')
    if req.status_code == requests.codes.ok:
        j = json.loads(req.text.encode(req.encoding))
        releases = j.get('releases', [])
        for release in releases:
            ver = parse(release)
            if not ver.is_prerelease:
                version = max(version, ver)
    # convert to a tuple of int
    ret = str(version).split('.')
    ret = [int(x) for x in ret]
    return tuple(ret)


def create_logfile(bt):
    with open(r'bintang/log.py', 'w') as flog:
        for idx, row in bt['/logging'].iterrows():
            flog.write(row['logging'] + '\n')


def create_logfile_dev(bt):
    with open(r'bintang/log.py', 'w') as flog:
        for idx, row in bt['/logging_dev'].iterrows():
            flog.write(row['logging_dev'] + '\n')


class Version:
    def __init__(self, ver_tuple):
        self.name = ver_tuple
        self.major = self.name[0]
        self.minor = self.name[1]
        self.patch = self.name[2]
    def get_next_patch(self):
        return (self.major, self.minor, self.patch + 1)

def clear_last_build():
    # delete directory bingtang.egg-info, build and dist
    BASE_DIR = Path(__file__).parent
    build_dir = ['bintang.egg-info','build','dist']
    build_path = ['{}'.format(BASE_DIR / x) for x in build_dir]
    for path in build_path:
        try:
            shutil.rmtree(path)
        except:
            pass


def create_setup_py(bt):
    # delete unwanted columns
    bt['/setup/setup_func'].drop_column('setup')
    bt['/setup/setup_func'].drop_column('setup_func')
       
    
    
    path = BASE_DIR / 'setup.py'
    with open(path, 'w') as f:
        for idx, row in bt['/setup/lines'].iterrows():
            #print(idx, row)
            f.write(row['lines'] + '\n')

        # start writing setup()
        f.write('setup(\n')
        setup_dict = bt['/setup/setup_func'].get_row_asdict(1)
        packages = bt['/setup/setup_func/packages'].to_list('packages')
        setup_dict['packages'] = packages

        for i, dictitem in enumerate(setup_dict.items(), start = 1):
            k,v = dictitem[0], dictitem[1]
            if k not in ['long_description']:
                v = repr(v)
            if i < len(setup_dict): # if not the last item
                line_towrite = ' '*4 + k + '=' + v + ',\n'
            else:
                line_towrite = ' '*4 + k + '=' + v + '\n'
            f.write(line_towrite)
        f.write(')') # close the setup function



# WARNING!!!!!!!
# user name __token__
# password apitoken

if __name__ == "__main__":
    bt = Bintang()
    setup_data = None
    with open(r'zetup_data.yaml', 'r') as stream:
        setup_data = yaml.safe_load(stream)

    # print(setup_data)
    bt.read_dict(setup_data)
   
    # print(bt)
    # re-create log.py to set log level as logging.ERROR
    create_logfile(bt)

    published_ver = get_version('bintang')
    ver = Version(published_ver)
    next_ver_str = '.'.join([str(x) for x in ver.get_next_patch()])

    
    
    # update version in the table
    bt['/setup/setup_func'].update_row(1, 'version', next_ver_str)

    
    # clear the last build
    clear_last_build()
    # create setup
    create_setup_py(bt)

    response = input('You are going to create setup with patch version {}? (C)ancel (P)roceed?: '.format(next_ver_str))
    if response.upper()=='C':
        quit()


    


    #sandbox.run_setup("setup.py",['sdist','--format=zip']) #python setup.py sdist
    

    # build a wheel package
    sandbox.run_setup("setup.py",['bdist_wheel'])
    myenv = os.environ.copy()
    subprocess.run(["python","-m","twine","upload","dist/*"], env=myenv, shell=True)
    create_logfile_dev(bt)
    print('finish')


