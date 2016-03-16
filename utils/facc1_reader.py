# coding: utf-8
import tarfile
from tarfile import TarInfo
import pandas as pd
import os
import logging

class FACC1Reader(object):
    COLUMNS = ['trec_id', 'encoding', 'entity', 'start', 'end', 'posterior', 'posterior_context_only', 'freebase_tag']
    
    def __init__(self, fpath):
        self.fpath = fpath
        logging.basicConfig(filename='facc1.log', level=logging.INFO)
    
    def __iter__(self):
        with tarfile.open(self.fpath, 'r:gz') as tar:
            for member in tar.getmembers():
                fname = os.path.basename(member.name)
                if fname.endswith('.tsv'):
                    f = tar.extractfile(member)
                    try:
                        df = pd.read_csv(f, sep='\t', header=None, names=self.COLUMNS)
                        logging.info('Finished: %s (%s)' % (fname, self.fpath))
                        yield (fname.replace('.tsv', ''), df)
                    except:
                        logging.warning('Error: %s (%s)' % (fname, self.fpath))