
from adapters import AdpBitfinexWSS

from pymongo import MongoClient

API_KEY     = None
API_SECRET  = None

if __name__ == "__main__":
#	websocket.enableTrace(True)
	db_client = MongoClient('localhost', 27017)
	wss = AdpBitfinexWSS("wss://api.bitfinex.com/ws/2",
						db_client, API_KEY, API_SECRET)
	wss.run_forever()

