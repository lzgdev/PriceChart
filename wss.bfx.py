
from adapters import AdpBitfinexWSS

API_KEY     = None
API_SECRET  = None

if __name__ == "__main__":
#	websocket.enableTrace(True)
	wss = AdpBitfinexWSS("wss://api.bitfinex.com/ws/2", API_KEY, API_SECRET)
	wss.run_forever()

