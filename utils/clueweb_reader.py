#-*- coding: utf-8 -*-
from __future__ import print_function
import warc
import re
import codecs

class ClueWebReader(object):
    
    def __init__(self, fpaths):
        # target file path
        if not isinstance(fpaths, list):
            fpaths = [fpaths]
        
        self.fpaths = fpaths
        
    def __getitem__(self, key):
        return self.record_tuples[key]
        
    def __len__(self):
        return len(self.record_tuples)
                
    def __iter__(self):
        for fpath in self.fpaths:
            f = warc.open(fpath)
            for record in f:
                if record.type == 'response':
                    try:
                        header = record.header
#                         content = record.payload.read()
                        content = unicode(record.payload.read(), encoding='utf-8', errors='replace')
                        yield (header, content)
                    except Exception as e:
                        print(e)
            f.close()
        
    def _get_content_length(self, record):
        string = record.payload.read()
        return int(re.search(self.PATTERN, string).group(1))
    
    def get_records(self):
        return iter(self.record_tuples)