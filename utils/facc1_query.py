#-*- coding: utf-8 -*-
from blaze import Data
from odo import odo
from pandas import DataFrame
import psycopg2

class FACC1Query(object):
    SERVER_URI = 'postgresql://postgres:Rudfhr88!@server.kyoungrok.com/facc1::{0}'
    DSHAPE = 'var * {trec_id: string, encoding: string, entity: ?string, start: int64, end: int64, posterior: float64, posterior_context_only: float64, tag: string}'
    
    def __init__(self, collection='clueweb12_00'):
        self.uri = self.SERVER_URI.format(collection) # ex: clueweb12_00
        self.facc1 = Data(self.uri)
        
    def __call__(self, trec_id):
        df = odo(self.facc1[self.facc1.trec_id == trec_id], DataFrame, dshape=self.DSHAPE).sort_values('start')
        return [tuple(r) for r in df.to_records(index=False)]