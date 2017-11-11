
from adapters import TradeBook_DbBase

from pymongo import MongoClient

API_KEY     = None
API_SECRET  = None

class TradeBook_DbRead(TradeBook_DbBase):
	def __init__(self, prec, size):
		super(TradeBook_DbRead, self).__init__(prec, size)

if __name__ == "__main__":
#	websocket.enableTrace(True)
	db_client = MongoClient('localhost', 27017)
	db_database = db_client['books']
	tb = TradeBook_DbRead("P0", 25)
	tb.dbInit(db_database)
	tb.dbLoad_Books()
	tb.dbgOut_Books()

