import subprocess
from setuptools import sandbox
import time

if __name__ == "__main__":

    sandbox.run_setup("setup.py",['sdist','--format=zip']) #python setup.py sdist
    try:
        import bintang
        subprocess.call('pip uninstall -y bintang')
    except ImportError:
        time.sleep(1)
        subprocess.call('pip uninstall -y bintang')
        print('New installation...')
    finally:
        subprocess.call('pip install "C:\\Users\\60145210\\Documents\\Projects\\bintang\\dist\\bintang-0.1.0.zip"')