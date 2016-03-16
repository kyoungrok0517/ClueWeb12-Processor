# coding: utf-8
from utils.warc_reader import WarcReader
import os

def test_recordreading():
    """ test_recordreading
    
    Allow +-3 difference with the real count 
    """
    THRESHOLD = 3
    
    # twitter
    reader = WarcReader('data/0000tw-00.warc.gz')
    count = 24644
    assert(len(reader) in range(count-THRESHOLD,count+THRESHOLD))
    
    # webpage
    reader = WarcReader('data/0000wb-00.warc.gz')
    count = 41355
    assert(len(reader) in range(count-THRESHOLD,count+THRESHOLD))
    
    # WikiTravel
    reader = WarcReader('data/0000wt-00.warc.gz')    
    count = 25444
    assert(len(reader) in range(count-THRESHOLD,count+THRESHOLD))