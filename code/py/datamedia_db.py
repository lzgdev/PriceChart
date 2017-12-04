import websocket
import _thread
import time

import hmac
import hashlib
import json

import pymongo

from pymongo import MongoClient

from adapters.basebooks import TradeBook_Base, TradeBook_DbBase
from adapters.basebooks import DBNAME_BOOK_PRICEEXTR, DBNAME_BOOK_RECTYPE

class TradeBook_DbWrite(TradeBook_DbBase):
	def __init__(self, prec, size):
		self.fdbg_dbwr = False
		super(TradeBook_DbWrite, self).__init__(prec, size)

	def dbInit(self, db_database):
		super(TradeBook_DbWrite, self).dbInit(db_database)
		if (self.db_collection == None):
			self.db_collection = pymongo.collection.Collection(db_database, self.wreq_prec, True)
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new collection(write).")
		if (self.db_collection != None) and (self.dbid_price_bids == None):
			self.dbid_price_bids = self.db_collection.insert_one({ DBNAME_BOOK_RECTYPE: 'price.extr.bids', }).inserted_id
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new price:bids(write).")
		if (self.db_collection != None) and (self.dbid_price_asks == None):
			self.dbid_price_asks = self.db_collection.insert_one({ DBNAME_BOOK_RECTYPE: 'price.extr.asks', }).inserted_id
			if self.fdbg_dbwr:
				print("TradeBook_DbWrite(dbInit): new price:asks(write).")

	def upBookRec_end_ex(self, rec_update, rec_del, rec_new, rec_up):
		price_rec = rec_update[0]
		flag_bids = True if rec_update[2] >  0.0 else False
		# locate the book record from self.loc_book_bids or self.loc_book_asks
		list_update = self.loc_book_bids if flag_bids else self.loc_book_asks
		# update database
		price_min = None if len(list_update) == 0 else list_update[0][0]
		price_max = None if len(list_update) == 0 else list_update[len(list_update)-1][0]
		if flag_bids:
			self.db_collection.find_one_and_update( { '_id': self.dbid_price_bids, },
					{ '$set': { DBNAME_BOOK_PRICEEXTR: [ price_min, price_max, ], }, })
		else:
			self.db_collection.find_one_and_update( { '_id': self.dbid_price_asks, },
					{ '$set': { DBNAME_BOOK_PRICEEXTR: [ price_min, price_max, ], }, })
		str_rec_type = 'book.bids' if flag_bids else 'book.asks'
		if   rec_del:
			self.db_collection.delete_one({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec, })
		elif rec_new:
			self.db_collection.insert_one({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec,
						'book-count': rec_update[1], 'book-amount': rec_update[2], })
		elif rec_up:
			self.db_collection.find_one_and_update({ DBNAME_BOOK_RECTYPE: str_rec_type, 'book-price': price_rec, },
						{ '$set': { 'book-count': rec_update[1], 'book-amount': rec_update[2], } })

	def upBookRecs_bgn_ex(self, recs_update):
		if (self.db_collection == None):
			return
		self.db_collection.delete_many({ DBNAME_BOOK_RECTYPE: 'book.bids', })
		self.db_collection.delete_many({ DBNAME_BOOK_RECTYPE: 'book.asks', })


