# coding: utf-8
import tarfile
from tarfile import TarInfo
import pandas as pd

class FACC1Reader(object):
    COLUMNS = ['trec_id', 'encoding', 'entity', 'start', 'end', 'posterior', 'posterior_context_only', 'freebase_tag']
    
    def __init__(self, fpath):
        self.fpath = fpath
    
    def __iter__(self):
        with tarfile.open(self.fpath, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.tsv'):
                    f = tar.extractfile(member)
                    df = pd.read_csv(f, sep='\t', header=None, names=self.COLUMNS)
                    yield (member.name.replace('.tsv', ''), df)