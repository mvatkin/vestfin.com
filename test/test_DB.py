import json
from unittest import TestCase
from datetime import datetime
from test.db_api import DB
from time import sleep

__author__ = 'maksim'


client_json = """
{
  "email": "vatkin.public@gmail.com",
  "fname": "Maksim",
  "lname": "Vatkin",
  "gender": "M",
  "createTS": "2015-09-26 12:00:00 UTC"
}
"""
trades_json = """
{
  "email": "vatkin.public@gmail.com",
  "symbol": "AAPL",
  "qty": "20",
  "ts": "2015-09-26 16:00:00.223 UTC"
}
"""


class TestDB(TestCase):

  def test_addClient(self):
    tst_db = DB('test2.db')
    tst_db.dropTables()
    if not (tst_db.addClient(client_json)):
      self.fail()

  def test_addSameClient(self):
    tst_db = DB('test2.db')
    tst_db.dropTables()
    if not (tst_db.addClient(client_json)):
      self.fail()
    if tst_db.addClient(client_json):
      self.fail()

  def test_getTradeTbl(self):
    tst_db = DB('test2.db')
    tst_db.dropTables()
    if not (tst_db.addTrade(trades_json)):
      self.fail()

  @staticmethod
  def makeTradeJsons(tj):
    j = json.loads(tj)
    newJsons = dict()
    for i in range(1,5):
      nj = {k:v  for k,v in j.items()}
      ts = datetime.now()
      nj['ts'] = ts.strftime("%Y-%m-%d %H:%M:%S.%f")
      sleep(0.05)
      newJsons[i] = json.dumps(nj)
    return newJsons

  def test_addTrade(self):
    nj = self.makeTradeJsons(trades_json)
    db = DB('test2.db')
    db.dropTables()
    for j in nj.values():
      if not (db.addTrade(j)):
        self.fail()

  def test_addPortfolio(self):
    nj = self.makeTradeJsons(trades_json)
    db = DB('test.db')
    db.dropTables()
    for j in nj.values():
      if not (db.addTrade(j)):
        self.fail()

    trades = '['
    for key, value in nj.iteritems():
      tr = json.loads(value)
      DB.setTradeId(tr)
      trades += ('' if key==1 else ', ') + str(tr['tradeId'])
    trades += ']'

    clientId = DB.setClientId(json.loads(nj[1]))['clientId']

    pf_j = '{"portfolioName": "My portfolio", "clientId": %s, "trades": %s }' % (clientId, trades)
    if not (db.addPortfolio(pf_j)):
      self.fail()
    if (db.addPortfolio(pf_j)):
      self.fail()
