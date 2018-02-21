
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
appMain = ktsave.CTDataContainer_Down1Out(logger)
appMain.downInit()

list_tasks_down = appMain.getDown_ExecCfg()

if dbg_main_loop:
	for dsrc_cfg in list_tasks_down:
		print("appMain, dsrc_cfg:", dsrc_cfg)

appMain.logger.info("Process(" + appMain.inf_this + ") begin ...")

appMain.execMain(list_tasks_down, prep='down1', post='down1')

appMain.logger.info("Process(" + appMain.inf_this + ") finish.")


