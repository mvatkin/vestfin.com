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
        self.trade = None
        self.portfolio_trades = None
        self.portfolio = None
        return

    @classmethod
    def dropTables(cls):
        cls.getClientTbl().drop()
        cls.getPortfolioTbl().drop()
        cls.getPortfolio2TradeTbl().drop()
        cls.getTradeTbl().drop();
        cls.vestfin_db.commit()
        cls.client = None
        cls.trade = None
        cls.portfolio_trades = None
        cls.portfolio = None


    @classmethod
    def getClientTbl(self, tblname = 'clients'):
        if self.client is None:
            self.client = self.vestfin_db.get_table(tblname, primary_id='clientId')
        return self.client

    @classmethod
    def addClient(self, client_json):
        try:
            self.getClientTbl().insert(self.parseJson(client_json))
            return True
        except Exception:
            logger.error('Failed to add client')
            return False

    @classmethod
    def getTradeTbl(self, tblname = 'trades'):
        if self.trade is None:
            self.trade = self.vestfin_db.get_table(tblname, primary_id='tradeId')
        return self.trade

    @classmethod
    def addTrade(self, trade_json):
        try:
            self.getTradeTbl().insert(self.setTradeId(self.parseJson(trade_json)))
            return True
        except Exception as e:
            #print(e)
            logger.error('Failed to add trade')
            return False

    @classmethod
    def getPortfolio2TradeTbl(self, tblname = 'portfolio_trades'):
        if self.portfolio_trades is None:
            self.portfolio_trades = self.vestfin_db.get_table(tblname, primary_id='tradeId')
        return self.portfolio_trades

    @classmethod
    def getPortfolioTbl(self, tblname = 'portfolios'):
        if self.portfolio is None:
            self.portfolio = self.vestfin_db.get_table(tblname, primary_id='portfolioId')
        return self.portfolio

    @classmethod
    def addPortfolio(self, pf_json):
        try:
            pf = self.parseJson(pf_json)
            pf = self.setIdField(pf,'portfolioId','portfolioName')

            p2t = self.getPortfolio2TradeTbl()
            pfId = pf['portfolioId']
            for t in pf['trades']:
                p2t.insert({'portfolioId':pfId, 'tradeId':t})

            self.getPortfolioTbl().insert({k:v for k,v in pf.items() if k != 'trades'})

            return True
        except Exception:
            logger.error('Failed to add portfolio')
            return False

    @classmethod
    def parseJson(cls, msg_json, idFieldName='clientId', idSrcField='email'):
        r = json.loads(msg_json)
        cls.setIdField(r, idFieldName, idSrcField)
        return r

    @staticmethod
    def setIdField(r, idFieldName, idSrcField):
        if not (r.has_key(idFieldName)):
            r[idFieldName] = int(hashlib.md5(r[idSrcField].encode()).hexdigest(), 16) % (10 ** 8)
        return r

    @staticmethod
    def setTradeId(r):
        if not (r.has_key('tradeId')):
            id_fld = r['ts'] + r['email']
            r['tradeId'] = int(hashlib.md5(id_fld.encode()).hexdigest(), 16) % (10 ** 8)
        return r


