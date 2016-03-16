# coding: utf-8
class FACC1Query(object):
    def __init__(self, uri='postgresql://postgres:Rudfhr88!@server.kyoungrok.com/facc1::clueweb12_00'):
        self.uri = uri
        self.facc1 = Data(self.uri)
        
    def get_entities(self, trec_id):
        df = odo(self.facc1[self.facc1.trec_id == trec_id], DataFrame)
        return [tuple(r) for r in df.to_records(index=False)]