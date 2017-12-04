
from adapters import TradeBook_DbBase

from pymongo import MongoClient

API_KEY     = None
API_SECRET  = None

if __name__ == "__main__":
	#websocket.enableTrace(True)
	appData_TB_P0 = TradeBook_DbBase("P0", 25)
	db_client = MongoClient('localhost', 27017)
	appData_TB_P0.dbInit(db_client['books'])
	appData_TB_P0.dbLoad_Books()
	appData_TB_P0.dbgOut_Books()

