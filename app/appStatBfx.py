
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


list_tasks_cfg = [
		{
			'name_chan': 'ticker',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('_id', -1)], },
		},
		{
			'name_chan': 'trades',
			'switch':     True,
			#'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD" },
			'load_args': { 'filter': { }, 'limit':  10, 'sort': [('_id', -1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('_id', -1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('_id', -1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('_id', -1)], },
		},
		{
			'name_chan': 'book',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('_id', -1)], },
		},
		{
			'name_chan': 'candles',
			#'switch':     True,
			'switch':    False,
			'wreq_args': { "key": "trade:1m:tBTCUSD" },
			'load_args': { 'filter': { }, 'limit':   1, 'sort': [('_id', -1)], },
		},
	]

list_tasks_run = []

for unit_task in list_tasks_cfg:
	if not unit_task['switch']:
		continue
	list_tasks_run.append(unit_task)


appMain = ktstat.CTDataContainer_StatOut(logger, None)

appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

appMain.execMain(list_tasks_run)

appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


