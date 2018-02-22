
import json
import http.client

import time

class CBfxHist(object):
	def __init__(self, mts_end):
		self.mts_end  = mts_end
		self.obj_json_resp  = None

		self.url_netloc = 'api.bitfinex.com'
		self.url_main_path = None
		self.url_req_path  = None
		self.idx_mts_rec = None

	def httpGet(self):
		self.url_req_path  = self.url_main_path + str(self.mts_end)

		http_conn = http.client.HTTPSConnection(self.url_netloc)

		http_conn.request("GET", self.url_req_path)
		http_resp = http_conn.getresponse()

		content_type = http_resp.getheader('Content-Type')

		print("Time, start:", str(self.mts_end))
		print("Resp, Status:", http_resp.status, ", reason:", http_resp.reason, ", Content-Type:", content_type)

		json_data = None
		if content_type != None:
			http_data = http_resp.read()
			#print("Data:", http_data)
			json_data = json.loads(http_data.decode('utf-8'))
		self.obj_json_resp  = json_data
		return self.obj_json_resp

	def dbgPrint(self, num_rec):
		print("Req:", self.url_req_path, ", num_resp:", len(self.obj_json_resp))
		for idx_data, item_data in enumerate(self.obj_json_resp):
			if idx_data >= num_rec:
				break
			mts_rec  = item_data[self.idx_mts_rec]
			mts_diff = mts_rec - self.mts_end
			print("Data:", str(idx_data).zfill(3), ", diff:", format(mts_diff, ","), item_data)


class CBfxHist_Trades(CBfxHist):
	def __init__(self, mts_end):
		super(CBfxHist_Trades, self).__init__(mts_end)
		#self.url_main_path  = '/v2/trades/tBTCUSD/hist?sort=1&start='
		self.url_main_path  = '/v2/trades/tBTCUSD/hist?&end='
		#limit_trades
		self.idx_mts_rec = 1

class CBfxHist_Candles(CBfxHist):
	def __init__(self, mts_end):
		super(CBfxHist_Candles, self).__init__(mts_end)
		#self.url_main_path  = '/v2/candles/trade:1m:tBTCUSD/hist?sort=1&start='
		#self.url_main_path  = '/v2/candles/trade:1m:tBTCUSD/hist?&end='
		self.url_main_path  = '/v2/candles/trade:1D:tBTCUSD/hist?&end='
		self.idx_mts_rec = 0

#Trades Begin,  mts: 1358182043000
#Candles Begin, mts: 1364774820000

# price: 1,000
mts_end = 1385639340000
# price: 2,000
#mts_end = 1497831120000
mts_end = 1498831120000
mts_end = 1498780800000

#mts_end = 1497831120000 + 60000

#mts_end = 1497833040000
#mts_end = 1497833700000
#mts_end = 1497833708000

num_req  = 0

flag_trades  = False
flag_candles = False

flag_trades  =  True
#flag_candles =  True

if flag_trades:
	if num_req >  0:
		time.sleep(5)
	bfxHist = CBfxHist_Trades(mts_end)
	bfxHist.httpGet()
	bfxHist.dbgPrint(500)
	num_req += 1

if flag_candles:
	if num_req >  0:
		time.sleep(5)
	bfxHist = CBfxHist_Candles(mts_end)
	bfxHist.httpGet()
	#bfxHist.dbgPrint(3)
	bfxHist.dbgPrint(500)
	num_req += 1

