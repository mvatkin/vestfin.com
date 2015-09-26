__author__ = 'maksim'
import db_api

class ClientPortfolios():
    """"""

    def __init__(self, db, client_email):
        """Constructor for client_portfolios"""
        self.db = db
        self.client_email = client_email
        self.client = db.getClientTbl().find_one(email = client_email)
        self.portfolios = db.getPortfolioTbl().find(clientId = self.client['clientId'])


    def getPortfolios(cls):
        cls.portfolios = cls.db.getPortfolioTbl().find(clientId = cls.client['clientId'])
        return cls.portfolios

    def getTrades(cls, portfolioName):
        pf = {p['portfolioName']:p for p in cls.getPortfolios()}
        pf = pf[portfolioName]
        return cls.db.getTrades(portfolioId=pf[db_api.PORTFOLIO_ID])
