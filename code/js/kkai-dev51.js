
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

class ClDataSet_ABooks_DbOut extends dev11.ClDataSet_ABooks
{
  constructor(db_writter, wreq_prec, wreq_len)
  {
    super(wreq_prec, wreq_len)
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
    if (this.loc_time_this <  this.loc_date_next)
    {
//    console.log("ClDataSet_ABooks_DbOut(onLocRecChg_CB) 11:", "book:", this.loc_name_coll, "doc:", JSON.stringify(book_rec));
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'update',
          time: this.loc_time_this,
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
      var  utc_new = Math.round(this.loc_time_this / this.loc_date_dur) * this.loc_date_dur;
      this.loc_name_coll  = 'book' + this.req_book_prec + '-' + _eval_name_coll(utc_new);
      this.loc_date_next  = utc_new + this.loc_date_dur;
//    console.log("ClDataSet_ABooks_DbOut(onLocRecChg_CB) 21:", "book:", this.loc_name_coll, "docs:", JSON.stringify(obj_docs));
      // add collection and docs
      this.loc_db_writter.dbOP_AddDoc(this.loc_name_coll, { type: 'snapshot',
          time: this.loc_time_this,
          data: obj_docs,
        });
      this.loc_db_writter.dbOP_AddColl(this.loc_name_coll);
    }
  }
}

class ClDataSet_ACandles_DbOut extends dev11.ClDataSet_ACandles
{
  constructor(recs_size, db_writter, wreq_key)
  {
    super(recs_size, wreq_key);
    this.loc_db_writter = db_writter;
    this.loc_gui_recn  = 0;
    this.loc_mts_sync  = -1;
    this.loc_sync_flag = false;
  }

  onSyncDataDB_impl()
  {
//    this.loc_db_writter.redraw({});
  }

  onLocAppendData_CB(chan_data)
  {
    if (!this.loc_sync_flag) {
      return 0;
    }
    this.onSyncDataDB_impl();
    this.loc_sync_flag = false;
  }

  onLocRecChg_CB(flag_sece, candle_rec, rec_index)
  {
    /*
    var gui_sers, pnt_this;
    gui_sers = this.loc_db_writter.series[0];
    pnt_this = [
        candle_rec.mts,
        candle_rec.open,
        candle_rec.high,
        candle_rec.low,
        candle_rec.close,
      ];
    if (candle_rec.mts >  this.loc_mts_sync) {
      gui_sers.addPoint(pnt_this, flag_sece);
      this.loc_mts_sync  = candle_rec.mts;
    }
    else {
      var gui_index = gui_sers.data.length + rec_index - this.loc_candle_recs.length;
      if ((gui_index >= 0) && (gui_index <  gui_sers.data.length)) {
        gui_sers.data[gui_index].update(pnt_this, flag_sece);
      }
    }
    gui_sers.addPoint(pnt_this, flag_sece);
    // */
//    console.log('ClDataSet_ACandles_DbOut(onLocRecChg_CB): rec=', candle_rec);
    this.loc_mts_sync  = candle_rec.mts;
    this.loc_sync_flag = true;
  }
}

module.exports = {
  ClDataSet_ACandles_DbOut: ClDataSet_ACandles_DbOut,
  ClDataSet_ABooks_DbOut:   ClDataSet_ABooks_DbOut,
}

