import json
import hashlib
import dataset
import logging

__author__ = 'maksim'

logger = logging.getLogger('DB')

class DB:
    @classmethod
    def __init__(self, dbName='vestfin.db'):
        self.vestfin_db = dataset.connect('sqlite:///%s' % dbName)
        self.client = None
        return

    @classmethod
    def openClientTbl(self, tblname = 'client'):
        self.client = self.vestfin_db.get_table(tblname, primary_id='md5Email')

    @classmethod
    def addClient(self, client_json):
        # try:
        # except Exception:
        #     logger.error('Failed to add client')
        #     return False
        r = json.loads(client_json)
        r['md5Email'] = int(hashlib.md5(r['email'].encode()).hexdigest(), 16) % (10 ** 8)
        if self.client is None:
            self.openClientTbl()
        self.client.insert(r)
        return True
