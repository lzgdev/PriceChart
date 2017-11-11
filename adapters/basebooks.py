
import pymongo

DBNAME_BOOK_RECTYPE    = 'rec-type'
DBNAME_BOOK_PRICEEXTR  = 'price-extr'

class TradeBook_Base(object):
	def __init__(self, prec, size):
		self.flag_dbg  = False
		self.wreq_prec = prec
		self.wreq_len  = size
		self.wrsp_chan = None
		self.loc_book_bids = []
		self.loc_book_asks = []

	def upBookChan(self, chan_id):
		self.wrsp_chan = chan_id

	def upBookRec(self, rec_update):
		price_rec = rec_update[0]
		flag_bids = True if rec_update[2] >  0.0 else False
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		list_update = self.loc_book_bids if flag_bids else self.loc_book_asks
		idx_bgn = 0
		idx_end = len(list_update) - 1
		while idx_bgn < idx_end:
			idx_rec = int((idx_bgn + idx_end) / 2)
			mid_price = list_update[idx_rec][0]
			if   price_rec <  mid_price:
				idx_end = idx_rec - 1
			elif price_rec >  mid_price:
				idx_bgn = idx_rec + 1
			else:
				idx_bgn = idx_end = idx_rec
		idx_rec = idx_end
		# delete/add/update book record in self.loc_book_bids or self.loc_book_asks
		rec_del = rec_new = rec_up = False
		if   idx_rec >= 0 and rec_update[1] == 0:
			rec_del = True
			if list_update[idx_rec][0] == price_rec:
				del list_update[idx_rec]
		elif idx_rec <  0  or price_rec > list_update[idx_rec][0]:
			rec_new = True
			idx_rec = idx_rec + 1
			list_update.insert(idx_rec, rec_update)
		elif price_rec < list_update[idx_rec][0]:
			rec_new = True
			list_update.insert(idx_rec, rec_update)
		else:
			rec_up  = True
			list_update[idx_rec] = rec_update
		self.upBookRec_ex(rec_update, rec_del, rec_new, rec_up)

	def upBookRecs(self, recs_update):
		if   type(recs_update[0]) is list:
			for rec_update in recs_update:
				self.upBookRec(rec_update)
			if self.flag_dbg:
				print("TradeBook Sn: ", json.dumps(self.loc_book_bids + self.loc_book_asks))
		else:
			rec_update = recs_update
			self.upBookRec(rec_update)
			if self.flag_dbg:
				print("TradeBook Up: ", json.dumps(rec_update), "=>", json.dumps(
						self.loc_book_bids if rec_update[2] > 0.0 else self.loc_book_asks))

	def upBookRec_ex(self, rec_update, rec_del, rec_new, rec_up):
		pass

	def dbgOut_Books(self):
		print("TradeBook(dbg) " + self.wreq_prec + ": bids=", self.loc_book_bids)
		print("TradeBook(dbg) " + self.wreq_prec + ": asks=", self.loc_book_asks)

class TradeBook_DbBase(TradeBook_Base):
	def __init__(self, db_database, prec, size):
		super(TradeBook_DbBase, self).__init__(prec, size)
		self.db_collection = None
		self.dbid_price_bids = None
		self.dbid_price_asks = None

	def dbInit(self, db_database):
		print("TradeBook_DbBase(dbInit): ...")
		flag_exist = True if (self.wreq_prec in db_database.collection_names(False)) else False
		self.db_collection = None if not flag_exist else pymongo.collection.Collection(
							db_database, self.wreq_prec, False)
		if self.db_collection == None:
			return
		rec_tmp = self.db_collection.find_one({ DBNAME_BOOK_RECTYPE: 'price.extr.bids', })
		if rec_tmp != None:
			self.dbid_price_bids = rec_tmp['_id']
		rec_tmp = self.db_collection.find_one({ DBNAME_BOOK_RECTYPE: 'price.extr.asks', })
		if rec_tmp != None:
			self.dbid_price_asks = rec_tmp['_id']

	def dbLoad_Books(self):
		if self.db_collection == None:
			return
		# load TradeBook of bids
		crr_recs = self.db_collection.find({ DBNAME_BOOK_RECTYPE: 'book.bids', })
		for doc_rec in crr_recs:
			rec_update = [ doc_rec['book-price'], doc_rec['book-count'], doc_rec['book-amount'], ]
			self.upBookRec(rec_update)
		crr_recs.close()
		# load TradeBook of asks
		crr_recs = self.db_collection.find({ DBNAME_BOOK_RECTYPE: 'book.asks', })
		for doc_rec in crr_recs:
			rec_update = [ doc_rec['book-price'], doc_rec['book-count'], doc_rec['book-amount'], ]
			self.upBookRec(rec_update)
		crr_recs.close()

