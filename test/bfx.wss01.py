import websocket

wss = websocket.WebSocket()

wss.connect('wss://api.bitfinex.com/ws/2')
#wss.send('{ "event":"ping" }')
wss.send('{ "event":"subscribe", "channel": "ticker", symbol: "tBTCUSD" }')

result = wss.recv()
print("Received(0): '%s'" % result)

"""
wss.send('{ "event":"ping" }')
result = wss.recv()
print("Received(11): '%s'" % result)
"""

#wss.send('{ "event":"subscribe", "channel": "trades", symbol: "tBTCUSD" }')
#wss.send('{ "event":"subscribe", "channel": "ticker", symbol: "tBTCUSD" }')
result = wss.recv()
print("Received(21): '%s'" % result)

wss.close()
