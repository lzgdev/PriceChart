
import os
import sys

import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../code')))

import ktdata
import ktstat

pid_root = os.getpid()

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

ktdata.CTDataContainer._gmap_TaskChans_init()

mts_off   = 1510685491435

mts_begin = 60*1000 * round(mts_off / (60*1000))
mts_end   = mts_begin + 60*1000

dsrc_db_trades = {
			'switch':     True,
			#'switch':    False,
			'dbg_stat':   3,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'sort': [('$natural', 1)], }, },
			],
		}

list_dsrc_cfg = [
		{
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'ticker', 'wreq_args': '{ "symbol": "tBTCUSD" }',
					'load_args': { 'limit':   1, 'sort': [('$natural', -1)], }, },
			],
		},
		{
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }',
					'load_args': { 'limit':   5, 'sort': [('$natural', -1)], }, },
			],
		},
		{
			'switch':    False,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':    'book', 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" }',
					'load_args': { 'limit':   1, 'sort': [('$natural', -1)], }, },
			],
		},
		{
			'switch':    False,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':    'book', 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" }',
					'load_args': { 'limit':   1, 'sort': [('$natural', -1)], }, },
			],
		},
		{
			'switch':    False,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':    'book', 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" }',
					'load_args': { 'limit':   1, 'sort': [('$natural', -1)], }, },
			],
		},
		{
			'switch':    False,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':    'book', 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" }',
					'load_args': { 'limit':   1, 'sort': [('$natural', -1)], }, },
			],
		},
		{
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }',
					'load_args': { 'limit':   1, 'sort': [('$natural', -1)], }, },
			],
		},
	]

list_dsrc_run = []

for dsrc_cfg in list_dsrc_cfg:
	if not dsrc_cfg.get('switch', True):
		continue
	list_dsrc_run.append(dsrc_cfg)

appMain = ktstat.CTDataContainer_StatOut(logger, None)
appMain.flag_dbg  =  True

appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

appMain.execMain(list_dsrc_run)

appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


