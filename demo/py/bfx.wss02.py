import websocket
import _thread
import time

def on_message(wss, message):
	print(message)

def on_error(wss, error):
	print(error)

def on_close(wss):
	print("### closed ###")

def on_open(wss):
#	wss.send('{ "event": "subscribe", "channel": "ticker", "symbol": "tBTCUSD" }')
#	wss.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "freq": "F0", "len": 25 }')
	wss.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P1", "freq": "F0", "len": 25 }')
#	wss.send('{ "event": "subscribe", "channel": "book", "symbol": "tBTCUSD", "prec": "P0", "freq": "F0", "len": 100 }')
	def run(*args):
		for i in range(15):
			time.sleep(1)
			print("wait ...")
#			wss.send('{ "event":"ping" }')
		time.sleep(1)
		wss.close()
		print("thread terminating...")
	_thread.start_new_thread(run, ())


if __name__ == "__main__":
	websocket.enableTrace(True)
	wss = websocket.WebSocketApp("wss://api.bitfinex.com/ws/2",
							on_message = on_message,
							on_error = on_error,
							on_close = on_close)
	wss.on_open = on_open
	wss.run_forever()

