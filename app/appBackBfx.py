
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

dbg_main_loop =  True
appMain = ktsave.CTDataContainer_BackOut(logger)
appMain.backInit()

while not appMain.flag_back_end:
	list_tasks_run = appMain.getBack_ExecCfg()

	if dbg_main_loop:
		for dsrc_cfg in list_tasks_run:
			print("appMain, dsrc_cfg:", dsrc_cfg)

	appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

	appMain.execMain(list_tasks_run, prep='back', post='back')

	appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


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


