# coding: utf-8
import tarfile
from tarfile import TarInfo
from glob import glob
import dask.bag as db
import dask.dataframe as dd
import dask
import os
import pandas as pd
from odo import odo
from dask.diagnostics import ProgressBar
from utils.facc1_reader import FACC1Reader


# # 출력 디렉토리 준비

# In[7]:

def get_output_name(fpath):
    return os.path.basename(fpath).replace('.tgz', '')

def get_output_dir(fpath):
    OUTPUT_DIR = '../../../Dataset/FACC1/output/'
    fname = get_output_name(fpath)
    return os.path.join(OUTPUT_DIR, fname)

def get_output_path(fpath, fname):
    output_dir = get_output_dir(fpath)
    return os.path.join(output_dir, "%s.csv.gz" % fname)
    
def prepare_output_dirs(fpaths):
    for fpath in fpaths:
        output_dir = get_output_dir(fpath)
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)


# In[19]:

def each_partition(values):
    for fpath in values:
        print('Processing: ', fpath)
        reader = FACC1Reader(fpath)
        for fname, df in reader:
            output_path = get_output_path(fpath, fname)
            odo(df, output_path)

def all_partition(values):
    pass


# In[22]:

fpaths = sorted(glob('../../../Dataset/FACC1/tgz/*.tgz'))
prepare_output_dirs(fpaths)
b = db.from_sequence(fpaths[:1]).reduction(each_partition, all_partition)
b.compute()

