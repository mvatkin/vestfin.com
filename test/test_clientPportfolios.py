from unittest import TestCase
from test.client_portfolios import ClientPortfolios
from test.db_api import DB
import test_DB
from test_DB import TestDB

__author__ = 'maksim'


class TestClientPportfolios(TestCase):
  def test_getTrades(self):
    nj = TestDB.makeTradeJsons()
    db = DB('test.db')
    db.dropTables()
    db.addClient(test_DB.client_json)
    pf_j = TestDB.fillPortfolioVars(db, nj)
    self.failIf(not (db.addPortfolio(pf_j)))

    cp = ClientPortfolios(db,'vatkin.public@gmail.com')
    a = [p['portfolioName'] for p in cp.getPortfolios()]
    self.failIf( len(a) <= 0)
    self.failIf(len([t['tradeId'] for t in cp.getTrades('My portfolio')]) <= 0)

