
import signal
import os
import sys
import time

import logging

import json

import threading
import multiprocessing

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../code')))

from ktdata import CTNetClient_BfxWss
from ktdata import CTDataSet_Ticker_DbIn, CTDataSet_Ticker_DbOut, CTDataSet_ABooks_DbIn, CTDataSet_ABooks_DbOut, CTDataSet_ACandles_DbIn, CTDataSet_ACandles_DbOut
from ktdata import KTDataMedia_DbReader, KTDataMedia_DbWriter

from pymongo import MongoClient

pid_parent = os.getpid()
pid_child  = 0

"""
def signal_hand(signum, frame):
	global flag_run
	pid_this = os.getpid()
	#if pid_parent != pid_this:
	#	return
	print("Signal handler called with signal=" + str(signum) + " in process=" + str(pid_this))
	flag_run = False

def sighand_chld(signum, frame):
	global pid_child
	print("Signal handler(child) with signal=" + str(signum))
	ret_wait = os.waitpid(pid_child, os.WNOHANG)
	print("Wait Child ret=" + str(ret_wait))
	if ret_wait[0] == pid_child:
		pid_child = 0

signal.signal(signal.SIGINT, signal_hand)
signal.signal(17, sighand_chld)

def child_run():
	global flag_run
	run_count = 0
	while flag_run:
		time.sleep(2)
		run_count += 1
		print("Child count: " + str(run_count) + " ...")

try:
	pid_child = os.fork()
except OSError:
	exit("Could not create a child process.")

if pid_child == 0:
	print("Child process that has the PID {}".format(os.getpid()))
	child_run()
	print("Child process finish.")
	exit()

print("In the parent process after forking the child {}".format(pid_child))
while pid_child > 0:
	time.sleep(1)
#finished = os.waitpid(pid_child, 0)
#print(finished)
"""

"""
API_KEY     = None
API_SECRET  = None

obj_netclient = None
obj_dbreader  = None
obj_dbwriter = None

flag_netclient = False

test_dataset_ticker = None
test_dataset_book = None
test_dataset_candles = None

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
logger = logging.getLogger()

timer_count = 0

def timer_entr():
	global timer_count
	timer_count += 1
	logger.info("Timer: count=" + str(timer_count))
	if (obj_netclient != None) and (timer_count > 100):
		obj_netclient.close()
	else:
		threading.Timer(2.0, timer_entr).start()

str_db_uri  = 'mongodb://127.0.0.1:27017'
str_db_name = 'bfx-pub'

flag_netclient = True

mapWREQs = [
		{ 'channel':  'ticker', 'uid': 'container-ticker', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', }, },
		{ 'channel':    'book', 'uid': 'container-bookP0', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P0', 'freq': 'F0', 'len': 100, }, },
		{ 'channel':    'book', 'uid': 'container-bookP1', 'visible': False, 'wreq_args': { 'symbol': 'tBTCUSD', 'prec': 'P1', 'freq': 'F0', 'len': 100, }, },
		{ 'channel': 'candles', 'uid': 'container-candle', 'visible':  True, 'wreq_args': { 'key': 'trade:1m:tBTCUSD', }, },
	]

if flag_netclient:
	url_bfx  = "wss://api.bitfinex.com/ws/2"
	obj_netclient = CTNetClient_BfxWss(logger, url_bfx)
	obj_dbwriter  = KTDataMedia_DbWriter(logger)
	obj_dbwriter.dbOP_Connect(str_db_uri, str_db_name)
else:
	obj_dbreader  = KTDataMedia_DbReader(logger)
	obj_dbreader.dbOP_Connect(str_db_uri, str_db_name)

for map_idx, map_unit in enumerate(mapWREQs):
	if not map_unit['visible']:
		continue
	obj_chan  = None
	logger.info("map idx=" + str(map_idx) + ", unit=" + str(map_unit))

	if map_unit['channel'] == 'ticker':
		if not flag_netclient:
			chan_obj = CTDataSet_Ticker_DbIn(logger, obj_dbreader, map_unit['wreq_args'])
		else:
			chan_obj = CTDataSet_Ticker_DbOut(logger, obj_dbwriter, map_unit['wreq_args'])
			test_dataset_ticker = chan_obj
	elif map_unit['channel'] == 'book':
		if not flag_netclient:
			chan_obj = CTDataSet_ABooks_DbIn(logger, obj_dbreader, map_unit['wreq_args'])
			test_dataset_book = chan_obj
		else:
			chan_obj = CTDataSet_ABooks_DbOut(logger, obj_dbwriter, map_unit['wreq_args'])
	elif map_unit['channel'] == 'candles':
		if not flag_netclient:
			chan_obj = CTDataSet_ACandles_DbIn(logger, obj_dbreader, 1000, map_unit['wreq_args'])
			test_dataset_candles = chan_obj
		else:
			chan_obj = CTDataSet_ACandles_DbOut(logger, obj_dbwriter, 1000, map_unit['wreq_args'])

	if chan_obj != None and obj_netclient != None:
		obj_netclient.addObj_DataReceiver(chan_obj)

timer_entr()

if flag_netclient:
	obj_netclient.run_forever()
elif test_dataset_ticker != None:
	#coll_name = "ticker-201712060400"
	coll_name = "ticker-201712060500"
	obj_dbreader.dbOP_LoadColl(coll_name, test_dataset_ticker, {}, [('mts', 1)], True)
elif test_dataset_book != None:
	coll_name = "book-P1-201712060330"
	obj_dbreader.dbOP_LoadColl(coll_name, test_dataset_book, {}, [('mts', 1)], True)
elif test_dataset_candles != None:
	coll_name = "candles-1m-201712060530"
	obj_dbreader.dbOP_LoadColl(coll_name, test_dataset_candles, {}, [('mts', 1)], True)
"""

print("Process id before forking: {}".format(pid_parent))

class Process_Net2Db(multiprocessing.Process):
	def __init__(self, e, d):
		super(Process_Net2Db, self).__init__()
		self.wait_e = e
		self.mgr_d = d

	def run(self):
		print("process running, pid:", os.getpid(), ", name:", self.name)
		self.wait_e.wait()
		print("process wait ...")
		self.mgr_d['proc'] = self.name
		time.sleep(2)
		print("process end.")

flag_run = True

e = multiprocessing.Event()
mgr = multiprocessing.Manager()
d = mgr.dict()

print("main: pid:", os.getpid(), ", d:", str(d))

t1 = Process_Net2Db(e, d)
t1.start()

print("main: waitting before calling Event.set()")

time.sleep(3)
e.set()

print("main: event is set.")

t1.join()

print("main: join. d=" + str(d))

