from unittest import TestCase
from user_api import DB


__author__ = 'maksim'

test_json = """
{
  "email": "vatkin.public@gmail.com",
  "fname": "Maksim",
  "lname": "Vatkin",
  "gender": "M",
  "createTS": "2015-09-26 12:00:00 UTC"
}
"""

class TestDB(TestCase):


  def test_openClientTbl(self):
    db = DB('test.db')
    db.openClientTbl()


  def test_addClient(self):
    tst_db = DB('test2.db')
    if not(tst_db.addClient(test_json)):
      self.fail()
