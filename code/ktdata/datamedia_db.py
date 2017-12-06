import websocket
import _thread
import time

import hmac
import hashlib
import json

import pymongo
import copy

COLLNAME_CollSet = 'set-colls'

class KTDataMedia_DbBase(object):
	def __init__(self, logger):
		self.logger = logger
		self.db_dburi  = None
		self.db_dbname = None
		self.db_client = None
		self.db_database    = None
		self.db_coll_set    = None
		self.db_collections = { }

	def dbChk_IsDbReady(self):
		return True if (self.db_database != None) else False

	def dbChk_IsCollReady(self, name_coll):
		if (name_coll == COLLNAME_CollSet) and (self.db_coll_set != None):
			return True
		if (name_coll != None) and (name_coll in self.db_collections):
			return True
		return False

	def dbOP_Connect(self, db_uri, db_name):
		if self.db_database != None:
			return True
		self.db_dburi  = db_uri
		self.db_dbname = db_name
		self.onDbOP_Connect_impl(self.db_dburi, self.db_dbname)

	def dbOP_Close(self):
		self.onDbOP_Close_impl()

	def dbOP_AddColl(self, name_coll, wreq_chan, wreq_args):
		if name_coll == None:
			return False
		if self.dbChk_IsCollReady(name_coll):
			return True
		return self.onDbOP_AddColl_impl(name_coll, wreq_chan, wreq_args)

	def dbOP_AddDoc(self, name_coll, obj_doc):
		if obj_doc == None:
			return False
		if not self.dbChk_IsCollReady(name_coll):
			return False 
		return self.onDbOP_AddDoc_impl(name_coll, obj_doc)

	def dbOP_LoadColl(self, name_coll, dataset, find_args, sort_args, flag_clean):
		if dataset == None:
			return False
		if not self.dbChk_IsCollReady(name_coll):
			self.dbOP_AddColl(name_coll, None, None)
		if not self.dbChk_IsCollReady(name_coll):
			return False
		return self.onDbOP_LoadColl_impl(name_coll, dataset, find_args, sort_args, flag_clean)

	def onDbEV_AddColl(self, name_coll):
		#self.logger.info("KTDataMedia_DbBase(onDbEV_AddColl): name_coll=" + name_coll)
		pass

	def onDbEV_AddDoc(self, name_coll, obj_doc, result):
		#self.logger.info("KTDataMedia_DbBase(onDbEV_AddDoc): name_coll=" + name_coll + ", obj_doc=" + str(obj_doc))
		pass

	def onDbEV_Closed(self):
		#self.logger.info("KTDataMedia_DbBase(onDbEV_Closed): db closed.")
		pass

	def onDbOP_Connect_impl(self, db_uri, db_name):
		self.db_client = pymongo.MongoClient(db_uri)
		self.db_database = pymongo.database.Database(self.db_client, db_name)
		self.dbOP_AddColl(COLLNAME_CollSet, None, None)

	def onDbOP_Close_impl(self):
		self.logger.info("KTDataMedia_DbBase(onDbOP_Close_impl): ...")
		if self.db_database == None:
			return False
		self.db_client.close()
		return True

	def onDbOP_AddColl_impl(self, name_coll, wreq_chan, wreq_args):
		db_coll = None
		if name_coll in self.db_database.collection_names(False):
			db_coll = pymongo.collection.Collection(self.db_database, name_coll, False)
			if   (db_coll != None) and (name_coll == COLLNAME_CollSet):
				self.db_coll_set = db_coll
			elif (db_coll != None) and (name_coll != COLLNAME_CollSet):
				self.db_collections[name_coll] = db_coll
		if db_coll == None:
			db_coll = pymongo.collection.Collection(self.db_database, name_coll, True)
			if   (db_coll != None) and (name_coll == COLLNAME_CollSet):
				self.db_coll_set = db_coll
			elif (db_coll != None) and (name_coll != COLLNAME_CollSet):
				self.db_collections[name_coll] = db_coll
				self.dbOP_AddDoc(COLLNAME_CollSet, {
							'coll': name_coll,
							'channel': wreq_chan,
							'reqargs': wreq_args,
						})
				self.onDbEV_AddColl(name_coll)
		return True

	def onDbOP_LoadColl_impl(self, name_coll, dataset, find_args, sort_args, flag_clean):
		db_coll = self.db_coll_set if (name_coll == COLLNAME_CollSet) else self.db_collections[name_coll]
		ret_cur = db_coll.find(find_args, None, 0, 0, False, pymongo.CursorType.NON_TAILABLE, sort_args)
		if flag_clean:
			dataset.locAppendData(1001, None)
		for obj_msg in ret_cur:
			db_doc  = copy.copy(obj_msg)
			del db_doc['_id']
			dataset.locAppendData(1001, db_doc)

	def onDbOP_AddDoc_impl(self, name_coll, obj_doc):
		db_coll = self.db_coll_set if (name_coll == COLLNAME_CollSet) else self.db_collections[name_coll]
		#self.logger.info("KTDataMedia_DbBase(onDbOP_AddDoc_impl): name_coll=" + name_coll + ", obj_doc=" + str(obj_doc))
		db_doc  = copy.copy(obj_doc)
		ret_ins = db_coll.insert_one(db_doc)
		if (name_coll != COLLNAME_CollSet):
			self.onDbEV_AddDoc(name_coll, db_doc, ret_ins)


class KTDataMedia_DbReader(KTDataMedia_DbBase):
	def __init__(self, logger):
		super(KTDataMedia_DbReader, self).__init__(logger)

class KTDataMedia_DbWriter(KTDataMedia_DbBase):
	def __init__(self, logger):
		super(KTDataMedia_DbWriter, self).__init__(logger)


