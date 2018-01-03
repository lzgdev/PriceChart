
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
from ktdata import CTDataSet_Ticker_DbOut, CTDataSet_ATrades_DbOut, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbOut
from ktdata import KTDataMedia_DbReader, KTDataMedia_DbWriter

from pymongo import MongoClient

pid_root = os.getpid()
flag_sig_usr1 = False
ntp_msec_off = 0

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

str_db_uri  = 'mongodb://127.0.0.1:27017'
str_db_name = 'bfx-pub'

mapTasks = [ {
	'class': 'task01',
	'msec_dur': 3 * 3600 * 1000, 'msec_pre': 20 * 1000,
	#'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	'switch':  True,
	'jobs': [
		{ 'channel':  'trades', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', }, },
		]
	},
    {
	'class': 'task02',
	'msec_dur': 3 * 3600 * 1000, 'msec_pre': 20 * 1000,
	#'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	'switch':  True,
	'jobs': [
		{ 'channel':  'ticker', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', }, },
		{ 'channel': 'candles', 'switch':  True, 'wreq_args': { 'key': 'trade:1m:tBTCUSD', }, },
		]
	},
    {
	'class': 'task03',
	'msec_dur': 3 * 3600 * 1000, 'msec_pre': 20 * 1000,
	#'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	'switch':  True,
	'jobs': [
		{ 'channel':    'book', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P0', 'freq': 'F1', 'len': '100', }, },
		{ 'channel':    'book', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P1', 'freq': 'F1', 'len': '100', }, },
		{ 'channel':    'book', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P2', 'freq': 'F1', 'len': '100', }, },
		{ 'channel':    'book', 'switch':  True, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P3', 'freq': 'F1', 'len': '100', }, },
		]
	},
  ]

print("Process id before forking: pid=" + str(pid_root))

def _util_msec_now():
	return int(round(time.time() * 1000))

class Process_Net2Db(multiprocessing.Process):
	tok_tasks = []
	tok_chans = []
	cnt_procs = []

	def __init__(self, logger, idx_task, token_new):
		super(Process_Net2Db, self).__init__()
		num_jobs = len(mapTasks[idx_task]['jobs'])
		# expand static members
		while len(self.tok_tasks) <= idx_task:
			self.tok_tasks.append(multiprocessing.Value('l', 0))
		while len(self.tok_chans) <= idx_task:
			self.tok_chans.append([])
		while len(self.tok_chans[idx_task]) <  num_jobs:
			self.tok_chans[idx_task].append(multiprocessing.Value('l', 0))
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
		self.logger.info("Process(" + self.info_app + ") begin ...")

		url_bfx  = "wss://api.bitfinex.com/ws/2"
		self.obj_netclient = CTNetClient_BfxWss(self.logger, self.tok_task, self.loc_token_this, url_bfx, ntp_msec_off)
		self.obj_dbwriter  = KTDataMedia_DbWriter(self.logger)
		self.obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)

		num_coll_msec  =  3 * 60 * 60 * 1000
		#num_coll_msec  =  1 * 60 * 60 * 1000

		for map_idx, map_unit in enumerate(mapTasks[self.idx_task]['jobs']):
			if not map_unit['switch']:
				continue
			obj_chan  = None
			#self.logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))

			if   map_unit['channel'] == 'ticker':
				obj_chan = CTDataSet_Ticker_DbOut(self.logger, self.obj_dbwriter, num_coll_msec, map_unit['wreq_args'])
			elif map_unit['channel'] == 'trades':
				obj_chan = CTDataSet_ATrades_DbOut(self.logger, self.obj_dbwriter, num_coll_msec, 512, map_unit['wreq_args'])
			elif map_unit['channel'] == 'book':
				obj_chan = CTDataSet_ABooks_DbOut(self.logger, self.obj_dbwriter, num_coll_msec, map_unit['wreq_args'])
			elif map_unit['channel'] == 'candles':
				obj_chan = CTDataSet_ACandles_DbOut(self.logger, self.obj_dbwriter, num_coll_msec, 512, map_unit['wreq_args'])

			if obj_chan != None:
				self.obj_netclient.addObj_DataReceiver(obj_chan, self.tok_chans[self.idx_task][map_idx])

		self.obj_netclient.run_forever()
		self.logger.info("Process(" + self.info_app + ") finish.")

flag_dbg_main  = False
flag_run_dbg01 = False

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
#flag_run_dbg01 =  True

#
# Main entrance
#
print("main(bgn): pid:", pid_root)
for t in range(0, len(mapTasks)):
	if mapTasks[t]['switch']:
		g_procs.append(Process_Net2Db(logger, t, 0))

# Main code(debug mode)
if flag_run_dbg01:
	for p in range(0, len(g_procs)):
		g_procs[p].run()
	print("main(dbg01): finish.")
	sys.exit(0)

# Main code(main part)
flag_main_init = True
ntp_syn_msec = 0

while len(g_procs) > 0:
	# sync time with netclient
	msec_now  = _util_msec_now()
	new_msec_off = None
	if msec_now >  ntp_syn_msec + 180000:
		try:
			ntp_clt = ntplib.NTPClient()
			ntp_ret_off  = ntp_clt.request('cn.pool.ntp.org', version=3).offset
			#print("NTP: ret_off=" + str(ntp_ret_off))
			new_msec_off = round(1000.0 * ntp_ret_off)
			print("NTP: msec_off=" + str(new_msec_off))
		except:
			new_msec_off = None
	if new_msec_off != None:
		ntp_msec_off = new_msec_off
		ntp_syn_msec = msec_now
	# continue regular child process maintain
	if flag_main_init:
		flag_main_init = False
		for p in range(0, len(g_procs)):
			g_procs[p].start()
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
		proc_new = Process_Net2Db(logger, proc_old.idx_task, proc_old.loc_token_next)
		g_procs.append(proc_new)
		proc_new.start()
	# join terminated child processes
	for p in range(num_procs-1, -1, -1):
		if g_procs[p].is_alive():
			continue
		proc_pop = g_procs.pop(p)
		proc_pop.join()
		if flag_dbg_main:
			print("main(chld) join process=" + str(proc_pop) + ", tok(task=" + str(proc_pop.tok_task) +
					",chans=" + str(proc_pop.tok_chans[proc_pop.idx_task]))
		del proc_pop

	# sleep
	time.sleep(2)


print("main(end): pid:", pid_root)

