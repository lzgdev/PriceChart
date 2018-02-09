
import copy
import os
import sys

import logging

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../code')))

import ktdata
import ktsave

pid_root = os.getpid()

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

ktdata.CTDataContainer._gmap_TaskChans_init()

dbg_main_loop = False

"""
dsrc_cfg_ticker =
		{
			'name_chan': 'ticker',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'ticker', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'filter': { }, 'limit':   0, 'sort': [('$natural', 1)], }, },
			],
		}
"""

dsrc_db_trades = {
			'switch':     True,
			#'switch':    False,
			'dbg_stat':   3,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'trades', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'sort': [('$natural', 1)], }, },
			],
		}

"""
dsrc_cfg_bookP0 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'ticker', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'filter': { }, 'limit':   0, 'sort': [('$natural', 1)], }, },
			],
		}

dsrc_cfg_bookP1 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'ticker', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'filter': { }, 'limit':   0, 'sort': [('$natural', 1)], }, },
			],
		}

dsrc_cfg_bookP2 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'ticker', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'filter': { }, 'limit':   0, 'sort': [('$natural', 1)], }, },
			],
		}

dsrc_cfg_bookP3 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" },
			'load_args': { 'sort': [('$natural', 1)], },
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel':  'ticker', 'wreq_args': '{ "symbol": "tBTCUSD" }', 'load_args': { 'filter': { }, 'limit':   0, 'sort': [('$natural', 1)], }, },
			],
			#'load_args': { 'filter': { }, 'limit':  50, 'sort': [('$natural', 1)], },
		}
"""

dsrc_db_candles = {
			'switch':     True,
			#'switch':    False,
			#'dbg_stat':   2,
			'url': 'mongodb://127.0.0.1:27017/bfx-pub',
			'chans': [
				{ 'channel': 'candles', 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', 'load_args': { 'sort': [('$natural', 1)], }, },
			],
		}

dbg_main_loop =  True
appMain = ktsave.CTDataContainer_BackOut(logger)


while not appMain.flag_back_end:
	list_tasks_run = []
	if dbg_main_loop:
		print("appMain, tid_last:", appMain.read_trades_tid_last, ", mts_last:", appMain.read_candles_mts_last)

	if appMain.read_trades_num >  0:
		dsrc_cfg = copy.copy(dsrc_db_trades)
		dsrc_db_load = dsrc_cfg['chans'][0]['load_args']
		if appMain.read_trades_tid_last == None:
			dsrc_db_load['filter'] = { 'mts': { '$gte': appMain.back_mts_now, } }
		else:
			dsrc_db_load['filter'] = { 'tid': { '$gt': appMain.read_trades_tid_last, } }
		dsrc_db_load['limit']  =  appMain.read_trades_num
		list_tasks_run.append(dsrc_cfg)

	if appMain.read_candles_num >  0:
		dsrc_cfg = copy.copy(dsrc_db_candles)
		dsrc_db_load = dsrc_cfg['chans'][0]['load_args']
		if appMain.read_candles_mts_last == None:
			dsrc_db_load['filter'] = { 'mts': { '$gte': appMain.back_mts_now, } }
		else:
			dsrc_db_load['filter'] = { 'mts': { '$gt': appMain.read_candles_mts_last, } }
		dsrc_db_load['limit']  = appMain.read_candles_num
		list_tasks_run.append(dsrc_cfg)

	if dbg_main_loop:
		for dsrc_cfg in list_tasks_run:
			print("appMain, dsrc_cfg:", dsrc_cfg)

	appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

	appMain.execMain(list_tasks_run, prep='back', post='back')

	appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


