
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

input_cfg_ticker =
		{
			'name_chan': 'ticker',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		}

input_cfg_trades =
		{
			'name_chan': 'trades',
			'switch':     True,
			#'switch':    False,
			'dbg_stat':   3,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'sort': [('$natural', 1)], },
		}

input_cfg_bookP0 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		}

input_cfg_bookP1 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		}

input_cfg_bookP2 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		}

input_cfg_bookP3 =
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" },
			'load_args': { 'sort': [('$natural', 1)], },
			#'load_args': { 'filter': { }, 'limit':  50, 'sort': [('$natural', 1)], },
		}

input_cfg_candles =
		{
			'name_chan': 'candles',
			'switch':     True,
			#'switch':    False,
			#'dbg_stat':   2,
			'wreq_args': { "key": "trade:1m:tBTCUSD" },
			'load_args': { 'sort': [('$natural', 1)], },
		}

#dbg_main_loop =  True
appMain = ktsave.CTDataContainer_BackOut(logger, None)


while not appMain.flag_stat_end:
	list_tasks_run = []
	if dbg_main_loop:
		print("appMain, tid_last:", appMain.read_trades_tid_last, ", mts_last:", appMain.read_candles_mts_last)

	if appMain.read_trades_num >  0:
		input_run = copy.copy(input_cfg_trades)
		if appMain.read_trades_tid_last == None:
			input_run['load_args']['filter'] = { 'mts': { '$gte': appMain.stat_mts_now, } }
		else:
			input_run['load_args']['filter'] = { 'tid': { '$gt': appMain.read_trades_tid_last, } }
		input_run['load_args']['limit']  =  appMain.read_trades_num
		list_tasks_run.append(input_run)

	if appMain.read_candles_num >  0:
		input_run = copy.copy(input_cfg_candles)
		if appMain.read_candles_mts_last == None:
			input_run['load_args']['filter'] = { 'mts': { '$gte': appMain.stat_mts_now, } }
		else:
			input_run['load_args']['filter'] = { 'mts': { '$gt': appMain.read_candles_mts_last, } }
		input_run['load_args']['limit']  = appMain.read_candles_num
		list_tasks_run.append(input_run)

	if dbg_main_loop:
		for input_run in list_tasks_run:
			print("appMain, input_run:", input_run)

	appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

	appMain.execMain(list_tasks_run, prep='back', post='back')

	appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


