
// module kkai-dev51
const dev11 = require('./kkai-dev11.js');
const dev52 = require('./kkai-dev52.js');

function _eval_name_coll(time_utc)
{
  var  date_str, name_tmp;
  date_str = new Date(time_utc).toISOString();
  name_tmp = date_str.slice( 0,  4) + date_str.slice( 5,  7) + date_str.slice( 8, 10) +
             date_str.slice(11, 13) + date_str.slice(14, 16);
  return name_tmp;
}

class ClDataSet_Ticker_DbOut extends dev11.ClDataSet_Ticker
{
  constructor(db_writter, wreq_args)
  {
    super(wreq_args);
    this.flag_loc_time  = true;
    this.loc_db_writter = db_writter;

    this.loc_date_dur   = 30 * 60 * 1000;
    this.loc_name_coll  = null;
    this.loc_date_next  = 0;
  }

  onLocRecChg_CB(ticker_rec, rec_index)
  {
    var utc_now;
    utc_now = this.loc_time_this;
    if (utc_now <  this.loc_date_next)
    {
//    console.log("ClDataSet_Ticker_DbOut(onLocRecChg_CB) 11:", "coll:", this.loc_name_coll, "doc:", JSON.stringify(ticker_rec));
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'update',
          time: utc_now,
          data: ticker_rec,
        });
    }
    else
    {
      // compose this.loc_name_coll
      var  utc_new = Math.floor(utc_now / this.loc_date_dur) * this.loc_date_dur;
      this.loc_name_coll  = 'ticker-' + _eval_name_coll(utc_new);
      this.loc_date_next  = utc_new + this.loc_date_dur;
      // add collection and docs
//    console.log("ClDataSet_Ticker_DbOut(onLocRecChg_CB) 21:", "coll:", this.loc_name_coll, "docs:", JSON.stringify(ticker_rec));
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'snapshot',
          time: utc_now,
          data: ticker_rec,
        });
      this.loc_db_writter.dbOP_AddColl(this.loc_name_coll);
    }
  }
}

class ClDataSet_ABooks_DbOut extends dev11.ClDataSet_ABooks
{
  constructor(db_writter, wreq_args)
  {
    super(wreq_args);
    this.flag_loc_time  = true;
    this.loc_db_writter = db_writter;

    this.loc_date_dur   = 30 * 60 * 1000;
    this.loc_name_coll  = null;
    this.loc_date_next  = 0;
  }

  onLocRecChg_CB(flag_sece, book_rec, flag_bids, idx_book, flag_del)
  {
    if (!flag_sece) {
      return;
    }
    var utc_now;
    utc_now = this.loc_time_this;
    if (utc_now <  this.loc_date_next)
    {
//    console.log("ClDataSet_ABooks_DbOut(onLocRecChg_CB) 11:", "coll:", this.loc_name_coll, "doc:", JSON.stringify(book_rec));
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'update',
          time: utc_now,
          data: book_rec,
        });
    }
    else
    {
      var  i, obj_docs = [];
      // compose obj_docs
      for (i=0; i < this.loc_book_bids.length; i++) {
        obj_docs.push(this.loc_book_bids[i]);
      }
      for (i=0; i < this.loc_book_asks.length; i++) {
        obj_docs.push(this.loc_book_asks[i]);
      }
      // compose this.loc_name_coll
      var  utc_new = Math.floor(utc_now / this.loc_date_dur) * this.loc_date_dur;
      this.loc_name_coll  = 'book-' + this.wreq_args.prec + '-' + _eval_name_coll(utc_new);
      this.loc_date_next  = utc_new + this.loc_date_dur;
//    console.log("ClDataSet_ABooks_DbOut(onLocRecChg_CB) 21:", "coll:", this.loc_name_coll, "docs:", JSON.stringify(obj_docs));
      // add collection and docs
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'snapshot',
          time: utc_now,
          data: obj_docs,
        });
      this.loc_db_writter.dbOP_AddColl(this.loc_name_coll);
    }
  }
}

class ClDataSet_ACandles_DbOut extends dev11.ClDataSet_ACandles
{
  constructor(recs_size, db_writter, wreq_args)
  {
    super(recs_size, wreq_args);
    this.loc_db_writter = db_writter;

    this.loc_date_dur   = 30 * 60 * 1000;
    this.loc_name_coll  = null;
    this.loc_date_next  = 0;
    this.loc_mts_sync   = 0;

    var i1, i2;
    i1  =  this.wreq_args.key.indexOf(':');
    i2  = (i1 < 0) ? -1 : this.wreq_args.key.indexOf(':', i1+1);
    this.loc_name_key   = ((i1 < 0) || (i2 < 0)) ? '' : this.wreq_args.key.slice(i1+1, i2);
  }

  onLocRecChg_CB(flag_sece, candle_rec, rec_index)
  {
    if (!flag_sece) {
      return;
    }
    if (this.loc_candle_recs.length <= 1) {
      return;
    }
    var utc_now;
    candle_rec = this.loc_candle_recs[this.loc_candle_recs.length - 2];
    utc_now = candle_rec.mts;
    if ((utc_now <  this.loc_date_next) && (utc_now >  this.loc_mts_sync))
    {
//      console.log("ClDataSet_ACandles_DbOut(onLocRecChg_CB) 11:", "coll:", this.loc_name_coll, "doc:", JSON.stringify(candle_rec));
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'update',
          time: utc_now,
          data: candle_rec,
        });
      this.loc_mts_sync   = utc_now;
    }
    else
    if  (utc_now >= this.loc_date_next)
    {
      var  obj_docs;
      // compose obj_docs
      obj_docs = this.loc_candle_recs.slice(0, this.loc_candle_recs.length-1);
      // compose this.loc_name_coll
      var  utc_new = Math.floor(utc_now / this.loc_date_dur) * this.loc_date_dur;
      this.loc_name_coll  = 'candles-' + this.loc_name_key + '-' + _eval_name_coll(utc_new);
      this.loc_date_next  = utc_new + this.loc_date_dur;
//      console.log("ClDataSet_ACandles_DbOut(onLocRecChg_CB) 21:", "coll:", this.loc_name_coll, "docs:", JSON.stringify(obj_docs));
      // add collection and docs
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'snapshot',
          time: utc_now,
          data: obj_docs,
        });
      this.loc_mts_sync   = utc_now;
      this.loc_db_writter.dbOP_AddColl(this.loc_name_coll);
    }
  }
}

module.exports = {
  ClDataSet_Ticker_DbOut:   ClDataSet_Ticker_DbOut,
  ClDataSet_ACandles_DbOut: ClDataSet_ACandles_DbOut,
  ClDataSet_ABooks_DbOut:   ClDataSet_ABooks_DbOut,
}

