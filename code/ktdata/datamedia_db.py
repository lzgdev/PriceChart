
import pymongo

import json
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

	def dbChk_Db_Ready(self):
		return True if (self.db_database != None) else False

	def dbChk_Coll_Ready(self, name_coll):
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

	def dbOP_CollAdd(self, name_coll, wreq_chan, wreq_args):
		if name_coll == None:
			return False
		if self.dbChk_Coll_Ready(name_coll):
			return True
		return self.onDbOP_CollAdd_impl(name_coll, wreq_chan, wreq_args)

	def dbOP_DocAdd(self, name_coll, obj_doc):
		if obj_doc == None:
			return None
		if not self.dbChk_Coll_Ready(name_coll):
			return None
		return self.onDbOP_DocAdd_impl(name_coll, obj_doc)

	def dbOP_DocFind_One(self, name_coll, filt_args, sort_args):
		if not self.dbChk_Coll_Ready(name_coll):
			return None
		db_coll = self.db_coll_set if (name_coll == COLLNAME_CollSet) else self.db_collections[name_coll]
		if sort_args == None:
			db_doc  = db_coll.find_one(filt_args)
		else:
			kwargs_find = { 'sort': sort_args, }
			db_doc  = db_coll.find_one(filt_args, **kwargs_find)
		return db_doc

	def dbOP_CollLoad(self, id_chan, name_coll, **kwargs):
		num_docs = -1
		if not self.dbChk_Coll_Ready(name_coll):
			self.dbOP_CollAdd(name_coll, None, None)
		self.onDbEV_CollLoad_Begin(id_chan)
		if self.dbChk_Coll_Ready(name_coll):
			num_docs = self.onDbOP_CollLoad_impl(id_chan, name_coll, **kwargs)
		self.onDbEV_CollLoad_End(id_chan, num_docs)

	def onDbEV_CollAdd(self, name_coll):
		#self.logger.info("KTDataMedia_DbBase(onDbEV_CollAdd): name_coll=" + name_coll)
		pass

	def onDbEV_DocAdd(self, name_coll, obj_doc, result):
		#self.logger.info("KTDataMedia_DbBase(onDbEV_DocAdd): name_coll=" + name_coll + ", obj_doc=" + str(obj_doc))
		pass

	def onDbEV_Closed(self):
		#self.logger.info("KTDataMedia_DbBase(onDbEV_Closed): db closed.")
		pass

	def onDbEV_CollLoad_Begin(self, id_chan):
		pass

	def onDbEV_CollLoad_Doc(self, id_chan, obj_doc):
		pass

	def onDbEV_CollLoad_End(self, id_chan, num_docs):
		pass

	def onDbOP_Connect_impl(self, db_uri, db_name):
		self.db_client = pymongo.MongoClient(db_uri)
		self.db_database = pymongo.database.Database(self.db_client, db_name)
		self.dbOP_CollAdd(COLLNAME_CollSet, None, None)

	def onDbOP_Close_impl(self):
		self.logger.info("KTDataMedia_DbBase(onDbOP_Close_impl): ...")
		if self.db_database == None:
			return False
		self.db_client.close()
		return True

	def onDbOP_CollAdd_impl(self, name_coll, wreq_chan, wreq_args):
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
				self.dbOP_DocAdd(COLLNAME_CollSet, {
							'coll': name_coll,
							'channel': wreq_chan,
							'reqargs': wreq_args,
						})
				db_coll.create_index([('_id', pymongo.ASCENDING), ('mts', pymongo.ASCENDING)])
				self.onDbEV_CollAdd(name_coll)
		return self.dbChk_Coll_Ready(name_coll)

	def onDbOP_CollLoad_impl(self, id_chan, name_coll, **kwargs):
		num_docs = 0
		arg_filter = kwargs.get('filter', None)
		arg_skip   = kwargs.get('skip',   0)
		arg_limit  = kwargs.get('limit',  0)
		arg_sort   = kwargs.get('sort',   None)
		db_coll = self.db_coll_set if (name_coll == COLLNAME_CollSet) else self.db_collections[name_coll]
		ret_cur = db_coll.find(arg_filter, None, arg_skip, arg_limit, False, pymongo.CursorType.NON_TAILABLE, arg_sort)
		for obj_msg in ret_cur:
			db_doc  = copy.copy(obj_msg)
			del db_doc['_id']
			self.onDbEV_CollLoad_Doc(id_chan, db_doc)
			num_docs += 1
		return num_docs

	def onDbOP_DocAdd_impl(self, name_coll, obj_doc):
		db_coll = self.db_coll_set if (name_coll == COLLNAME_CollSet) else self.db_collections[name_coll]
		#self.logger.info("KTDataMedia_DbBase(onDbOP_DocAdd_impl): name_coll=" + name_coll + ", obj_doc=" + str(obj_doc))
		db_doc  = copy.copy(obj_doc)
		ret_ins = db_coll.insert_one(db_doc)
		if (name_coll != COLLNAME_CollSet):
			self.onDbEV_DocAdd(name_coll, db_doc, ret_ins)
		return db_doc if ret_ins.acknowledged else None


class KTDataMedia_DbReader(KTDataMedia_DbBase):
	def __init__(self, logger, obj_dbinput):
		super(KTDataMedia_DbReader, self).__init__(logger)
		self.obj_dbinput = obj_dbinput

	def onDbEV_CollLoad_Begin(self, id_chan):
		#self.logger.info("DataMedia(load): begin, chan=" + str(id_chan))
		if self.obj_dbinput != None:
			self.obj_dbinput.datFwd_Begin(id_chan)

	def onDbEV_CollLoad_Doc(self, id_chan, obj_doc):
		#self.logger.info("DataMedia(load): chan=" + str(id_chan) + ", doc=" + str(obj_doc))
		if self.obj_dbinput != None:
			self.obj_dbinput.datFwd_Doc(id_chan, obj_doc)

	def onDbEV_CollLoad_End(self, id_chan, num_docs):
		#self.logger.info("DataMedia(load): end, chan=" + str(id_chan) + ", num=" + str(num_docs))
		if self.obj_dbinput != None:
			self.obj_dbinput.datFwd_End(id_chan, num_docs)


class KTDataMedia_DbWriter(KTDataMedia_DbBase):
	def __init__(self, logger):
		super(KTDataMedia_DbWriter, self).__init__(logger)


