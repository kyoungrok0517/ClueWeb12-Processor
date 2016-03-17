# coding: utf-8
from blaze import Data
from odo import odo
from pandas import DataFrame
import psycopg2

class FACC1Query(object):
    SERVER_URI = 'postgresql://facc1:Rudfhr88!@server.kyoungrok.com/facc1::{0}'
    
    def __init__(self, collection):
        self.uri = self.SERVER_URI.format(collection) # clueweb12_00
        self.facc1 = Data(self.uri)
        
    def __call__(self, trec_id):
        df = odo(self.facc1[self.facc1.trec_id == trec_id], DataFrame)
        return [tuple(r) for r in df.to_records(index=False)]