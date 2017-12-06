
import os
import sys
import logging

import json

import threading

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../code')))

#from adapters import AdpBitfinexWSS
from ktdata import CTNetClient_BfxWss
from ktdata import CTDataSet_Ticker_DbIn, CTDataSet_Ticker_DbOut, CTDataSet_ABooks_DbIn, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbIn, CTDataSet_ACandles_DbOut
from ktdata import KTDataMedia_DbReader, KTDataMedia_DbWriter

from pymongo import MongoClient

API_KEY     = None
API_SECRET  = None

obj_netclient = None
obj_dbreader  = None
obj_dbwriter = None

flag_netclient = False

test_dataset_ticker = None
test_dataset_book = None
test_dataset_candles = None

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

timer_count = 0

def timer_entr():
	global timer_count
	timer_count += 1
	logger.info("Timer: count=" + str(timer_count))
	if (obj_netclient != None) and (timer_count > 100):
		obj_netclient.close()
	else:
		threading.Timer(2.0, timer_entr).start()

str_db_uri  = 'mongodb://127.0.0.1:27017'
str_db_name = 'bfx-pub'

#flag_netclient = True

mapWREQs = [
		{ 'channel':  'ticker', 'uid': 'container-ticker', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', }, },
		{ 'channel':    'book', 'uid': 'container-bookP0', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P0', 'freq': 'F0', 'len': 100, }, },
		{ 'channel':    'book', 'uid': 'container-bookP1', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P1', 'freq': 'F0', 'len': 100, }, },
		{ 'channel': 'candles', 'uid': 'container-candle', 'visible':  True, 'wreq_args': { 'key': 'trade:1m:tBTCUSD', }, },
	]

if flag_netclient:
	url_bfx  = "wss://api.bitfinex.com/ws/2"
	obj_netclient = CTNetClient_BfxWss(logger, url_bfx)
	obj_dbwriter  = KTDataMedia_DbWriter(logger)
	obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
else:
	obj_dbreader  = KTDataMedia_DbReader(logger)
	obj_dbreader.dbOP_Connect(str_db_uri, str_db_name)

for map_idx, map_unit in enumerate(mapWREQs):
	if not map_unit['visible']:
		continue
	obj_chan  = None
	logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))

	if map_unit['channel'] == 'ticker':
		if not flag_netclient:
			chan_obj = CTDataSet_Ticker_DbIn(logger, obj_dbreader, map_unit['wreq_args'])
		else:
			chan_obj = CTDataSet_Ticker_DbOut(logger, obj_dbwriter, map_unit['wreq_args'])
			test_dataset_ticker = chan_obj
	elif map_unit['channel'] == 'book':
		if not flag_netclient:
			chan_obj = CTDataSet_ABooks_DbIn(logger, obj_dbreader, map_unit['wreq_args'])
			test_dataset_book = chan_obj
		else:
			chan_obj = CTDataSet_ABooks_DbOut(logger, obj_dbwriter, map_unit['wreq_args'])
	elif map_unit['channel'] == 'candles':
		if not flag_netclient:
			chan_obj = CTDataSet_ACandles_DbIn(logger, obj_dbreader, 1000, map_unit['wreq_args'])
			test_dataset_candles = chan_obj
		else:
			chan_obj = CTDataSet_ACandles_DbOut(logger, obj_dbwriter, 1000, map_unit['wreq_args'])

	if chan_obj != None and obj_netclient != None:
		obj_netclient.addObj_DataReceiver(chan_obj)

timer_entr()

if flag_netclient:
	obj_netclient.run_forever()
elif test_dataset_ticker != None:
	#coll_name = "ticker-201712060400"
	coll_name = "ticker-201712060500"
	obj_dbreader.dbOP_LoadColl(coll_name, test_dataset_ticker, {}, [('mts', 1)], True)
elif test_dataset_book != None:
	coll_name = "book-P1-201712060330"
	obj_dbreader.dbOP_LoadColl(coll_name, test_dataset_book, {}, [('mts', 1)], True)
elif test_dataset_candles != None:
	coll_name = "candles-1m-201712060530"
	obj_dbreader.dbOP_LoadColl(coll_name, test_dataset_candles, {}, [('mts', 1)], True)

