
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


dbg_main_loop = 0
appMain = ktstat.CTDataContainer_StatOut(logger, None)
appMain.flag_stat =  True
appMain.statInit()

#dbg_main_loop = 2

while not appMain.flag_stat_end:
	list_dsrc_stat = appMain.getStat_ExecCfg()
	if dbg_main_loop >= 2:
		for cfg_dsrc in list_dsrc_stat:
			print("appMain, cfg_dsrc:", str(cfg_dsrc))

	if dbg_main_loop >= 1:
		appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

	appMain.execMain(list_dsrc_stat, prep='stat11', post='stat11')

	if dbg_main_loop >= 1:
		appMain.logger.info("Process(" + appMain.inf_this + ") finish.")

	#appMain.flag_stat_end =  True

appMain.statShow()

