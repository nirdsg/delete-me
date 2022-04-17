
from contextlib import contextmanager
import os
import shutil


@contextmanager
def temp_directory(dir_name = 'temp', **kwds):
    
    os.makedirs(dir_name, exist_ok=True)
    
    try:
        yield dir_name

    except Exception:
        print(f"Unable to create '{dir_name}'")

    finally:
        shutil.rmtree(dir_name)
