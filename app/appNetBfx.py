
import signal
import os
import sys
import time
import math

import logging

import multiprocessing
import ntplib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../code')))

from ktdata import CTNetClient_BfxWss
from ktdata import CTDataSet_Ticker_DbIn, CTDataSet_Ticker_DbOut, CTDataSet_ABooks_DbIn, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbIn, CTDataSet_ACandles_DbOut
from ktdata import KTDataMedia_DbReader, KTDataMedia_DbWriter

from pymongo import MongoClient

pid_root = os.getpid()
flag_sig_usr1 = False

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

str_db_uri  = 'mongodb://127.0.0.1:27017'
str_db_name = 'bfx-pub'

mapTasks = [ {
	'class': 'task01',
	#'msec_dur': 1800 * 1000, 'msec_pre': 10 * 1000,
	'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	'jobs': [
		{ 'channel':  'ticker', 'switch': False, 'wreq_args': { 'symbol': 'tBTCUSD', }, },
		{ 'channel':    'book', 'switch': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P0', 'freq': 'F0', 'len': 100, }, },
		{ 'channel':    'book', 'switch': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P1', 'freq': 'F0', 'len': 100, }, },
		{ 'channel': 'candles', 'switch':  True, 'wreq_args': { 'key': 'trade:1m:tBTCUSD', }, },
		]
	}, ]

print("Process id before forking: {}".format(pid_root))

def _util_msec_now():
	return int(round(time.time() * 1000))

class Process_Net2Db(multiprocessing.Process):
	tok_tasks = []
	cnt_procs = []

	def __init__(self, logger, idx_task, token_new):
		super(Process_Net2Db, self).__init__()
		# expand static members
		while len(self.tok_tasks) <= idx_task:
			self.tok_tasks.append(multiprocessing.Value('l', 0))
		while len(self.cnt_procs) <= idx_task:
			self.cnt_procs.append(0)
		self.cnt_procs[idx_task] += 1
		# init local members
		self.logger = logger
		self.idx_task = idx_task
		self.tok_task = self.tok_tasks[self.idx_task]
		self.pid_this = None
		self.info_app = None
		self.obj_netclient = None
		self.obj_dbwriter  = None

		self.name = 'Net2Db' + str(self.idx_task) + '-' + str(self.cnt_procs[self.idx_task])

		unit_task = mapTasks[self.idx_task]
		msec_num  = unit_task['msec_dur']
		msec_pre  = unit_task['msec_pre']
		msec_now  = _util_msec_now()
		# regulate value of token_new
		if token_new <= 0:
			token_new = msec_now
		token_new = math.floor(token_new / msec_num) * msec_num
		self.loc_token_this  = token_new
		self.loc_token_next  = token_new + msec_num
		self.loc_token_invk  = token_new + msec_num - msec_pre

	def run(self):
		self.pid_this = os.getpid()
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
		self.obj_netclient = CTNetClient_BfxWss(self.logger, self.tok_task, self.loc_token_this, url_bfx)
		self.obj_dbwriter  = KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)

		for map_idx, map_unit in enumerate(mapTasks[self.idx_task]['jobs']):
			if not map_unit['switch']:
				continue
			obj_chan  = None
			self.logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))

			if   map_unit['channel'] == 'ticker':
				obj_chan = CTDataSet_Ticker_DbOut(self.logger, self.obj_dbwriter, map_unit['wreq_args'])
			elif map_unit['channel'] == 'book':
				obj_chan = CTDataSet_ABooks_DbOut(self.logger, self.obj_dbwriter, map_unit['wreq_args'])
			elif map_unit['channel'] == 'candles':
				obj_chan = CTDataSet_ACandles_DbOut(self.logger, self.obj_dbwriter, 1000, map_unit['wreq_args'])

			if obj_chan != None:
				self.obj_netclient.addObj_DataReceiver(obj_chan)

		self.obj_netclient.run_forever()
#		"""
		self.logger.info("Process(" + self.info_app + ") finish.")

flag_dbg_main  = False
flag_run_dbg01 =  True

g_procs = []

# Signal handler
_sighand_intr_orig  = signal.getsignal( 2)
def _sighand_intr(signum, frame):
	global pid_root
	global _sighand_intr_orig
	pid_this = os.getpid()
	if pid_root != pid_this:
		return
	print("Signal handler(intr) with signal=" + str(signum) + " in process=" + str(pid_this))
	_sighand_intr_orig(signum, frame)

def _sighand_usr2(signum, frame):
	print("Signal handler(usr2) with signal=" + str(signum) + ", tasks=" + str(len(g_procs)))
	flag_sig_usr1 = True

#signal.signal( 2, _sighand_intr)
signal.signal(12, _sighand_usr2)

flag_dbg_main  =  True
flag_run_dbg01 = False

#
# Main entrance
#
print("main(bgn): pid:", pid_root)
for t in range(0, len(mapTasks)):
	g_procs.append(Process_Net2Db(logger, t, 0))

# Main code(debug mode)
if flag_run_dbg01:
	for t in range(0, len(g_procs)):
		g_procs[t].run()
	print("main(dbg01): finish.")
	sys.exit(0)

# Main code(main part)
for t in range(0, len(g_procs)):
	g_procs[t].start()

#num_procs = 3
num_procs = 2
while len(g_procs) > 0:
	num_procs = len(g_procs)
	if flag_dbg_main:
		print("main(step): num=" + str(num_procs) + " ...")
	# invoke next child process
	msec_now  = _util_msec_now()
	for p in range(0, num_procs):
		proc_old = g_procs[p]
		if proc_old.loc_token_invk <= 0  or msec_now <  proc_old.loc_token_invk:
			continue
		if flag_dbg_main:
			print("main(p=" + str(p) + "): invk=" + str(proc_old.loc_token_invk) +
					", now=" + str(msec_now) + ", next=" + str(proc_old.loc_token_next))
		proc_old.loc_token_invk = 0
		if num_procs <= 0:
			continue
		proc_new = Process_Net2Db(logger, proc_old.idx_task, proc_old.loc_token_next)
		g_procs.append(proc_new)
		proc_new.start()
		num_procs -= 1
	# join terminated child processes
	for p in range(num_procs-1, -1, -1):
		if g_procs[p].is_alive():
			continue
		proc_pop = g_procs.pop(p)
		proc_pop.join()
		if flag_dbg_main:
			print("main(chld) join process=" + str(proc_pop) + ", tok(task=" + str(proc_pop.tok_task) + ")")
		del proc_pop
	# sleep
	time.sleep(2)

print("main(end): pid:", pid_root)

