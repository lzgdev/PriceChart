
import copy
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

mts_off   = 1510685520000
mts_dur   = 60*1000

mts_begin = 60*1000 * round(mts_off / (60*1000))
mts_end   = mts_begin + mts_dur

dbg_main_loop = False

list_tasks_cfg = [
		{
			'name_chan': 'ticker',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		},
		{
			'name_chan': 'trades',
			'switch':     True,
			#'switch':    False,
			'dbg_stat':   3,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'filter': { 'mts': { '$gte': mts_begin, } }, 'limit':  10, 'sort': [('$natural', 1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('$natural', 1)], },
			#'load_args': { 'filter': { }, 'limit':  50, 'sort': [('$natural', 1)], },
		},
		{
			'name_chan': 'candles',
			'switch':     True,
			#'switch':    False,
			#'dbg_stat':   2,
			'wreq_args': { "key": "trade:1m:tBTCUSD" },
			'load_args': { 'filter': { 'mts': { '$gte': mts_begin, } }, 'limit':   1, 'sort': [('$natural', 1)], },
		},
	]

#dbg_main_loop =  True
appMain = ktstat.CTDataContainer_StatOut(logger, None)


while not appMain.flag_stat_end:
	list_tasks_run = []
	if dbg_main_loop:
		print("appMain, tid_last:", appMain.read_trades_tid_last, ", mts_last:", appMain.read_candles_mts_last)

	for task_cfg in list_tasks_cfg:
		flag_run = False
		task_run = copy.copy(task_cfg)
		if task_run['name_chan'] ==  'trades' and  appMain.read_trades_num >  0:
			if appMain.read_trades_tid_last == None:
				task_run['load_args']['filter'] = { 'mts': { '$gte': appMain.stat_mts_now, } }
			else:
				task_run['load_args']['filter'] = { 'tid': { '$gt': appMain.read_trades_tid_last, } }
			task_run['load_args']['limit']  =  appMain.read_trades_num
			flag_run =  True
		if task_run['name_chan'] == 'candles' and appMain.read_candles_num >  0:
			if appMain.read_candles_mts_last == None:
				task_run['load_args']['filter'] = { 'mts': { '$gte': appMain.stat_mts_now, } }
			else:
				task_run['load_args']['filter'] = { 'mts': { '$gt': appMain.read_candles_mts_last, } }
			task_run['load_args']['limit']  = appMain.read_candles_num
			flag_run =  True
		if not flag_run:
			continue
		if dbg_main_loop:
			print("appMain, task_run:", task_run)
		list_tasks_run.append(task_run)

	appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

	appMain.execMain(list_tasks_run, prep='stat11', post='stat11')

	appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


