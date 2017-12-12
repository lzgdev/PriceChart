
import signal
import os
import sys
import time

import logging

import multiprocessing
import ntplib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../code')))

from ktdata import CTNetClient_BfxWss
from ktdata import CTDataSet_Ticker_DbIn, CTDataSet_Ticker_DbOut, CTDataSet_ABooks_DbIn, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbIn, CTDataSet_ACandles_DbOut
from ktdata import KTDataMedia_DbReader, KTDataMedia_DbWriter

from pymongo import MongoClient

pid_root = os.getpid()

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

str_db_uri  = 'mongodb://127.0.0.1:27017'
str_db_name = 'bfx-pub'

mapWREQs = [
		{ 'channel':  'ticker', 'uid': 'container-ticker', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', }, },
		{ 'channel':    'book', 'uid': 'container-bookP0', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P0', 'freq': 'F0', 'len': 100, }, },
		{ 'channel':    'book', 'uid': 'container-bookP1', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P1', 'freq': 'F0', 'len': 100, }, },
		{ 'channel': 'candles', 'uid': 'container-candle', 'visible':  True, 'wreq_args': { 'key': 'trade:1m:tBTCUSD', }, },
	]

print("Process id before forking: {}".format(pid_root))

class Process_Net2Db(multiprocessing.Process):
	def __init__(self, logger, chld_stat):
		super(Process_Net2Db, self).__init__()
		self.logger = logger
		self.proc_stat = chld_stat
		self.pid_this = None
		self.info_app = None
		self.obj_netclient = None
		self.obj_dbwriter  = None

	def run(self):
		self.pid_this = os.getpid()
		self.proc_stat['pid'] = self.pid_this
		self.info_app = "pid=" + str(self.pid_this) + ", name=" + self.name
		# debug code
		self.logger.info("Process(" + self.info_app + ") running ...")

		#c = ntplib.NTPClient()
		#r = c.request('europe.pool.ntp.org', version=3)
		#print("NTP: offset=" + str(r.offset))

		# create netclient and db writer object
		time.sleep(5)
#		"""
		url_bfx  = "wss://api.bitfinex.com/ws/2"
		self.obj_netclient = CTNetClient_BfxWss(self.logger, url_bfx)
		self.obj_dbwriter  = KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)

		for map_idx, map_unit in enumerate(mapWREQs):
			if not map_unit['visible']:
				continue
			obj_chan  = None
			self.logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))

			if map_unit['channel'] == 'ticker':
				obj_chan = CTDataSet_Ticker_DbOut(self.logger, self.obj_dbwriter, map_unit['wreq_args'])
			elif map_unit['channel'] == 'book':
				obj_chan = CTDataSet_ABooks_DbOut(self.logger, self.obj_dbwriter, map_unit['wreq_args'])
			elif map_unit['channel'] == 'candles':
				obj_chan = CTDataSet_ACandles_DbOut(self.logger, self.obj_dbwriter, 1000, map_unit['wreq_args'])

			if obj_chan != None:
				self.obj_netclient.addObj_DataReceiver(obj_chan)

		self.obj_netclient.run_forever()
#		"""
		self.proc_stat['exit'] = True
		self.logger.info("Process(" + self.info_app + ") finish.")

flag_run_dbg01 = True

g_smgr  = multiprocessing.Manager()
g_tasks = []

# Signal handler
def _sighand_intr(signum, frame):
	pid_this = os.getpid()
	if pid_root != pid_this:
		return
	print("Signal handler(intr) with signal=" + str(signum) + " in process=" + str(pid_this))

def _sighand_chld(signum, frame):
	pid_this = os.getpid()
	if pid_root != pid_this:
		return
	print("Signal handler(chld) with signal=" + str(signum) + ", tasks=" + str(len(g_tasks)))
	for idx_task in range(len(g_tasks)-1, -1, -1):
		if (g_tasks[idx_task].proc_stat['exit']):
			task_pop = g_tasks.pop(idx_task)
			print("Signal handler(chld) join process=" + str(task_pop) + ", stat=" + str(task_pop.proc_stat))
			task_pop.join()
			del task_pop

signal.signal( 2, _sighand_intr)
signal.signal(17, _sighand_chld)

g_tasks.append(Process_Net2Db(logger, g_smgr.dict({ 'keep':  True, 'exit': False, 'pid': 0, })))

flag_run_dbg01 = False

if flag_run_dbg01:
	for t in range(0, len(g_tasks)):
		g_tasks[t].run()
	print("main(dbg01): finish.")
else:
	print("main(bgn): pid:", pid_root)
	for t in range(0, len(g_tasks)):
		g_tasks[t].start()
	while len(g_tasks) > 0:
		print("main: wait ...")
		time.sleep(2)
	print("main(end): pid:", pid_root)

