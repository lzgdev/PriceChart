
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

appMain = ktstat.CTDataContainer_StatOut(logger, None)

num_run = 1
for idx_run in range(num_run):
	list_tasks_run = []

	for task_cfg in list_tasks_cfg:
		if not task_cfg['switch']:
			continue
		task_run = copy.copy(task_cfg)
		if task_run['name_chan'] == 'trades' and appMain.stat_tid_last != None:
			task_run['load_args']['filter'] = { 'tid': { '$gt': appMain.stat_tid_last, } }
			#task_run['load_args']['limit']  = 20
		print("task_run:", task_run)
		list_tasks_run.append(task_run)

	appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

	appMain.execMain(list_tasks_run, prep='stat01', post='stat01')

	appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


