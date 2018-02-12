
import json
import http.client

import time

class CBfxHist(object):
	def __init__(self, mts_start):
		self.mts_start  = mts_start
		self.obj_json_resp  = None

		self.url_netloc = 'api.bitfinex.com'
		self.url_main_path  = None
		self.idx_mts_rec = None

	def httpGet(self):
		url_path = self.url_main_path + str(self.mts_start)

		http_conn = http.client.HTTPSConnection(self.url_netloc)

		http_conn.request("GET", url_path)
		http_resp = http_conn.getresponse()

		content_type = http_resp.getheader('Content-Type')

		print("Time, start:", str(self.mts_start))
		print("Resp, Status:", http_resp.status, ", reason:", http_resp.reason, ", Content-Type:", content_type)

		json_data = None
		if content_type != None:
			http_data = http_resp.read()
			#print("Data:", http_data)
			json_data = json.loads(http_data.decode('utf-8'))
		self.obj_json_resp  = json_data
		return self.obj_json_resp

	def dbgPrint(self, num_rec):
		for idx_data, item_data in enumerate(self.obj_json_resp):
			if idx_data >= num_rec:
				break
			mts_rec  = item_data[self.idx_mts_rec]
			mts_diff = mts_rec - self.mts_start
			print("Data:", str(idx_data).zfill(3), ", off:", format(mts_diff, ","), item_data)


class CBfxHist_Trades(CBfxHist):
	def __init__(self, mts_start):
		super(CBfxHist_Trades, self).__init__(mts_start)
		self.url_main_path  = '/v2/trades/tBTCUSD/hist?sort=1&start='
		self.idx_mts_rec = 1

class CBfxHist_Candles(CBfxHist):
	def __init__(self, mts_start):
		super(CBfxHist_Candles, self).__init__(mts_start)
		self.url_main_path  = '/v2/candles/trade:1m:tBTCUSD/hist?sort=1&start='
		self.idx_mts_rec = 0

# price: 1,000
#mts_start = 1385639340000
# price: 2,000
mts_start = 1497831120000

bfxHist = CBfxHist_Candles(mts_start)
bfxHist.httpGet()
bfxHist.dbgPrint(3)

time.sleep(5)

bfxHist = CBfxHist_Trades(mts_start)
bfxHist.httpGet()
bfxHist.dbgPrint(500)

