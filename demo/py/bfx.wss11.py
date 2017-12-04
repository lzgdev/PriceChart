
from adapters import AdpBitfinexWSS

from pymongo import MongoClient

API_KEY     = None
API_SECRET  = None

if __name__ == "__main__":
#	websocket.enableTrace(True)
	url_bfx = "wss://api.bitfinex.com/ws/2"
	sio_namespace = "/bfx.books.P0"
	db_client = MongoClient('localhost', 27017)
	wss = AdpBitfinexWSS(url_bfx, sio_namespace, db_client, API_KEY, API_SECRET)
	wss.run_forever()

