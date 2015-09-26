import json
import hashlib
import dataset
import logging
import sqlalchemy
import pandas as pd

TRADE_SYMBOL = 'symbol'

PORTFOLIO_ID = 'portfolioId'
TRADE_ID = 'tradeId'
CLIENT_ID = 'clientId'

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
            self.client = self.vestfin_db.get_table(tblname, primary_id=CLIENT_ID)
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
            self.trade = self.vestfin_db.get_table(tblname, primary_id=TRADE_ID)
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
            self.portfolio_trades = self.vestfin_db.get_table(tblname, primary_id=TRADE_ID)
            self.portfolio_trades.create_column(PORTFOLIO_ID, sqlalchemy.Integer)
        return self.portfolio_trades

    @classmethod
    def getPortfolioTbl(self, tblname = 'portfolios'):
        if self.portfolio is None:
            self.portfolio = self.vestfin_db.get_table(tblname, primary_id=PORTFOLIO_ID)
            self.portfolio.create_column('portfolioName', sqlalchemy.VARCHAR)
            self.portfolio.create_column(CLIENT_ID, sqlalchemy.INTEGER)
        return self.portfolio

    @classmethod
    def addPortfolio(self, pf_json):
        try:
            p2t_t = self.getPortfolio2TradeTbl()
            pf_t = self.getPortfolioTbl()
            self.vestfin_db.begin()
            pf = self.parseJson(pf_json)
            pf = self.setIdField(pf,PORTFOLIO_ID,'portfolioName')

            pfId = pf[PORTFOLIO_ID]

            self.checkPositionForShorts(pf, pfId)

            for t in pf['trades']:
                p2t_t.insert({PORTFOLIO_ID:pfId, TRADE_ID:t})

            pf_t.insert({k:v for k,v in pf.items() if k != 'trades'})
            self.vestfin_db.commit()
            return True
        except Exception as e:
            self.vestfin_db.rollback()
            logger.error('Failed to add portfolio')
            return False

    @classmethod
    def checkPositionForShorts(cls, pf, pfId):
        storedPFQty = cls.getPortfolioPositionQty(pfId)
        newPosQty = cls.getPositionQty(pf['trades'])
        newPos = newPosQty.append(storedPFQty).groupby('symbol').sum()
        for p in newPos['sumQty']:
            if p < 0:
                raise Exception('Short position detected')

    @classmethod
    def getPortfolioPositionQty(cls, pfId):
        pf_qty = cls.vestfin_db.query(
            """
            SELECT t.symbol, sum(t.qty) sumQty
            FROM portfolio_trades pt
            JOIN trades t
                ON pt.tradeId = t.tradeId and pt.portfolioId=%s
            GROUP BY t.symbol
            """ % pfId)
        df = pd.DataFrame([r for r in pf_qty])
        return df

    @classmethod
    def getPositionQty(cls, trades):
        tr_str = ''
        for t in trades:
            tr_str += ('' if len(tr_str) == 0 else ', ') + str(t)
        pf_qty = cls.vestfin_db.query(
            """
            SELECT t.symbol, sum(t.qty) sumQty
            FROM trades t
            WHERE t.tradeId in (%s)
            GROUP BY t.symbol
            """ % tr_str)
        df = pd.DataFrame([r for r in pf_qty])
        return df

    @classmethod
    def parseJson(cls, msg_json):
        r = json.loads(msg_json)
        cls.setClientId(r)
        return r

    @staticmethod
    def setIdField(r, idFieldName, idSrcField):
        if not (r.has_key(idFieldName)):
            r[idFieldName] = int(hashlib.md5(r[idSrcField].encode()).hexdigest(), 16) % (10 ** 8)
        return r

    @staticmethod
    def setTradeId(r):
        if not (r.has_key(TRADE_ID)):
            id_fld = r['ts'] + r['email']
            r[TRADE_ID] = int(hashlib.md5(id_fld.encode()).hexdigest(), 16) % (10 ** 8)
        return r


    @staticmethod
    def setClientId(r):
        return DB.setIdField(r, CLIENT_ID, 'email')
