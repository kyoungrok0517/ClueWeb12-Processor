
# coding: utf-8

# In[42]:
from __future__ import unicode_literals
from __future__ import print_function
import warc
from bs4 import BeautifulSoup

def remove_boilerplate(html):
    soup = BeautifulSoup(html, 'html.parser')
    for script in soup(["script", "style"]):
        script.extract()
    text = soup.get_text()
    
    # break into lines and remove leading and trailing space on each
    # start from 18th line to get rid of WARC info lines
    lines = (line.strip() for line in text.splitlines()[18:])
    
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    
    # drop blank lines
    text = ' '.join(chunk for chunk in chunks if chunk)
    
    return text

# Record Counts (http://www.lemurproject.org/clueweb12/specs.php)
class ClueWebReader(object):
    HEADER_OFFSET = 157

    def __init__(self, fpath):
        # target file path
        self.fpath = fpath
        
    def __getitem__(self, key):
        return self.record_tuples[key]
        
    def __len__(self):
        return len(self.record_tuples)
                
    def __iter__(self):
        f = warc.open(self.fpath)
        for record in f:
            if record.type == 'response':
                yield (record.header['WARC-TREC-ID'], record.payload)
        f.close()
    
    def get_records(self):
        return iter(self.record_tuples)

