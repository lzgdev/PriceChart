
import signal
import os
import sys
import time
import math

import logging

import multiprocessing

import ntplib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../code')))

import ktdata
import ktstat

pid_root = os.getpid()
flag_sig_usr1 = False
ntp_msec_off = 0

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

ktdata.CTDataContainer._gmap_TaskChans_init()

mapTasks = [ {
	'class': 'task01',
	'msec_dur': 3 * 3600 * 1000, 'msec_pre': 20 * 1000,
	#'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	#'switch': False,
	'switch':  True,
	#'url': 'wss://api.bitfinex.com/ws/2',
	'url': 'https://api.bitfinex.com/v2',
	'jobs': [
		{ 'channel':  'trades', 'switch':  True, 'wreq_args': '{ "symbol": "tBTCUSD" }', },
		{ 'channel': 'candles', 'switch':  True, 'wreq_args': '{ "key": "trade:1m:tBTCUSD" }', },
		]
	},
    {
	'class': 'task02',
	'msec_dur': 3 * 3600 * 1000, 'msec_pre': 20 * 1000,
	#'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	'switch':  True,
	'url': 'wss://api.bitfinex.com/ws/2',
	'jobs': [
		{ 'channel':  'ticker', 'switch':  True, 'wreq_args': '{ "symbol": "tBTCUSD" }', },
		]
	},
    {
	'class': 'task03',
	'msec_dur': 3 * 3600 * 1000, 'msec_pre': 20 * 1000,
	#'msec_dur':   30 * 1000, 'msec_pre': 10 * 1000,
	'switch':  True,
	'url': 'wss://api.bitfinex.com/ws/2',
	'jobs': [
		{ 'channel':    'book', 'switch':  True, 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P0", "freq": "F1", "len": "100" }', },
		{ 'channel':    'book', 'switch':  True, 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P1", "freq": "F1", "len": "100" }', },
		{ 'channel':    'book', 'switch':  True, 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P2", "freq": "F1", "len": "100" }', },
		{ 'channel':    'book', 'switch':  True, 'wreq_args': '{ "symbol": "tBTCUSD", "prec": "P3", "freq": "F1", "len": "100" }', },
		]
	},
  ]

class Process_Net2Db(multiprocessing.Process, ktstat.CTDataContainer_StatOut):
	cnt_procs = []

	def __init__(self, logger, idx_task):
		multiprocessing.Process.__init__(self)
		ktstat.CTDataContainer_StatOut.__init__(self, logger, None)
		self.tok_mono_next = 0
		num_jobs = len(mapTasks[idx_task]['jobs'])
		# expand static members
		while len(self.cnt_procs) <= idx_task:
			self.cnt_procs.append(0)
		self.cnt_procs[idx_task] += 1
		# init local members
		self.logger = logger
		self.idx_task = idx_task
		self.info_app = None

		self.name = 'Net2Db' + str(self.idx_task) + '-' + str(self.cnt_procs[self.idx_task])

		# update self.tok_mono_next
		unit_task = mapTasks[self.idx_task]
		msec_next = unit_task['msec_dur'] if 'msec_dur' in unit_task else -1
		if msec_next > 0:
			self.tok_mono_next  = self.tok_mono_this + msec_next

	def run(self):
		self.info_app = "pid=" + str(self.pid_this) + ", name=" + self.name
		self.logger.info("Process(" + self.info_app + ") begin ...")

		task_unit = mapTasks[self.idx_task]

		#self.execMain(url=task_unit['url'], jobs=task_unit['jobs'], msec_off=ntp_msec_off)
		self.execMain(name_chan='candles', wreq_args={ "key": "trade:1m:tBTCUSD" })
		self.logger.info("Process(" + self.info_app + ") finish.")

dbg_dbg_main  = False
dbg_run_task  = -1

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

# debug settings
dbg_dbg_main  =  True
dbg_run_task  = 0

#
# Main entrance
#
print("main(bgn): pid:", pid_root)
for t in range(0, len(mapTasks)):
	if dbg_run_task >= 0 and t != dbg_run_task and mapTasks[t]['switch']:
		mapTasks[t]['switch'] = False
	if mapTasks[t]['switch']:
		g_procs.append(Process_Net2Db(logger, t))

# Main code(debug mode)
if dbg_run_task >= 0:
	for p in range(0, len(g_procs)):
		g_procs[p].run()
	print("main(dbg01): finish.")
	sys.exit(0)

# Main code(main part)
flag_main_init = True
ntp_syn_msec = 0

while len(g_procs) > 0:
	# sync time with netclient
	mono_now  = ktdata.CTDataContainer.mtsNow_mono()
	new_msec_off = None
	if mono_now >  ntp_syn_msec + 180000:
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
		ntp_syn_msec = mono_now
	# continue regular child process maintain
	if flag_main_init:
		flag_main_init = False
		for p in range(0, len(g_procs)):
			g_procs[p].start()
	num_procs = len(g_procs)
	if dbg_dbg_main:
		print("main(step): num=" + str(num_procs) + ", now=" + format(mono_now, ',') + " ...")
	# invoke next child process
	mono_now  = ktdata.CTDataContainer.mtsNow_mono()
	for p in range(0, num_procs):
		proc_old = g_procs[p]
		if proc_old.tok_mono_next <= 0  or mono_now <  proc_old.tok_mono_next:
			continue
		if dbg_dbg_main:
			print("main(p=" + str(p) + "): next=" + str(proc_old.tok_mono_next) +
					", now=" + str(mono_now))
		proc_old.tok_mono_next = 0
		proc_new = Process_Net2Db(logger, proc_old.idx_task)
		g_procs.append(proc_new)
		proc_new.start()
	# join terminated child processes
	for p in range(num_procs-1, -1, -1):
		if g_procs[p].is_alive():
			continue
		proc_pop = g_procs.pop(p)
		proc_pop.join()
		if dbg_dbg_main:
			print("main(chld) join process=" + str(proc_pop) + ", pid=" + str(proc_pop.pid_this) +
						", tok=" + str(proc_pop.tok_mono_this))
		del proc_pop

	# sleep
	time.sleep(2)


print("main(end): pid:", pid_root)

