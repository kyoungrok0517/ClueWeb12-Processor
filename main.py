
# coding: utf-8

# In[ ]:

import tarfile
from tarfile import TarInfo
# import dask.bag as db
# import dask.dataframe as dd
# import dask
import os
import pandas as pd
from odo import odo
# from dask.diagnostics import ProgressBar
from utils.facc1_reader import FACC1Reader

if __name__ == '__main__':
    # Start a progress bar for all computations
    # pbar = ProgressBar()
    # pbar.register()

    OUTPUT_DIR = './output/'
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)

    columns = ['trec_id', 'encoding', 'entity', 'start', 'end', 'posterior', 'posterior_context_only', 'freebase_tag']
    fpath = '../../../Data/ClueWeb12_00.tgz'

    reader = FACC1Reader(fpath)
    for i, f in enumerate(reader):
        df = pd.read_csv(f, sep='\t', header=None, names=columns)
        output_path = os.path.join(OUTPUT_DIR, "%s.csv.gz" % (i+1))
        odo(df, output_path)

