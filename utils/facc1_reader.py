import tarfile
from tarfile import TarInfo

class FACC1Reader(object):
    def __init__(self, fpath):
        self.fpath = fpath
    
    def __iter__(self):
        with tarfile.open(self.fpath, 'r:gz') as tar:
            for member in tar.getmembers():
                if member.name.endswith('.tsv'):
                    yield tar.extractfile(member)