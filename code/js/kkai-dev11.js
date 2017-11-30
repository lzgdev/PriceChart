// module kkai-dev11

class ClDataSet_Base
{
  constructor(name_chan, wreq_args)
  {
    this.name_chan = name_chan;
    this.chan_id = null;
    this.wreq_args = wreq_args;
    this.flag_loc_time  = false;
    this.loc_time_this  = 0;
  }

  locSet_ChanId(chan_id)
  {
    this.chan_id = chan_id;
  }

  locCleanData()
  {
    this.onLocCleanData_impl();
    this.onLocCleanData_CB();
  }

  locAppendData(obj_msg)
  {
    if (this.flag_loc_time) {
      this.loc_time_this = Date.now();
    }
    this.onLocAppendData_impl(obj_msg);
    this.onLocAppendData_CB(null);
  }

  locRecChg(flag_sece, obj_rec)
  {
    this.onLocRecChg_impl(flag_sece, obj_rec)
  }

  onLocCleanData_CB()
  {
  }
  onLocAppendData_CB(chan_data)
  {
  }

  onLocCleanData_impl()
  {
  }

  onLocAppendData_impl(obj_msg)
  {
  }

  onLocRecChg_impl(flag_sece, obj_rec)
  {
  }
}

class ClDataSet_Array extends ClDataSet_Base
{
  constructor(name_chan, wreq_args)
  {
    super(name_chan, wreq_args);
  }

  onLocAppendData_impl(obj_msg)
  {
    var data_msg = obj_msg[1];
    if (!Array.isArray(data_msg))
    {
    }
    else
    {
      if (!Array.isArray(data_msg[0]))
      {
        this.locRecChg(true, data_msg);
      }
      else
      {
        var  i, i_end;
        this.locCleanData(data_msg);
        i_end = data_msg.length-1;
        for (i=0; i <  i_end; i++) {
          this.locRecChg(false, data_msg[i]);
        }
        if (i_end >= 0) {
          this.locRecChg( true, data_msg[i]);
        }
      }
    }
  }
}

function _eval_book_unit(str_prec)
{
  var  book_unit = 1.0;
  if (str_prec == 'P0') {
    book_unit = Number(  0.1).toFixed(1);
  }
  else
  if (str_prec == 'P1') {
    book_unit = Number(  1.0);
  }
  else
  if (str_prec == 'P2') {
    book_unit = Number( 10.0);
  }
  else
  if (str_prec == 'P3') {
    book_unit = Number(100.0);
  }
  return book_unit;
}

class ClDataSet_Ticker extends ClDataSet_Base
{
  constructor(wreq_args)
  {
    super('ticker', wreq_args);
  }

  onLocAppendData_impl(obj_msg)
  {
    var data_msg = obj_msg[1];
    if (Array.isArray(data_msg) && data_msg.length == 10)
    {
      var  ticker_rec = {
          bid:        Number(data_msg[0]),
          bid_size:   Number(data_msg[1]),
          ask:        Number(data_msg[2]),
          ask_size:   Number(data_msg[3]),
          daily_change:   Number(data_msg[4]),
          daily_change_perc:  Number(data_msg[5]),
          last_price: Number(data_msg[6]),
          volume:     Number(data_msg[7]),
          high:       Number(data_msg[8]),
          low:        Number(data_msg[9]),
        };
      this.onLocRecChg_CB(ticker_rec, 0);
    }
  }

  onLocRecChg_CB(ticker_rec, rec_index)
  {
  }
}

class ClDataSet_ABooks extends ClDataSet_Array
{
  constructor(wreq_args)
  {
    super("book", wreq_args);
    this.loc_book_unit  = _eval_book_unit(this.wreq_args.prec);
    this.loc_book_bids  = [];
    this.loc_book_asks  = [];
  }

  onLocCleanData_impl()
  {
    this.loc_book_bids.length = 0;
    this.loc_book_asks.length = 0;
  }

  onLocRecChg_impl(flag_sece, obj_rec)
  {
    var  flag_bids  = (obj_rec[2] >  0.0) ? true : false;
    var  amount_rec = Number(flag_bids ? obj_rec[2] : (0.0 - obj_rec[2]));
    var  book_rec = {
        price:  Number(obj_rec[0]),
        type:   flag_bids ? 'bid' : 'ask',
        count:  obj_rec[1],
        amount: amount_rec,
        sumamt: 0.0,
      };
    var  idx_book, idx_bgn, idx_end;
    var  flag_del;
    var  book_recs = flag_bids ? this.loc_book_bids : this.loc_book_asks;
    // locate the book record from self.loc_book_bids or self.loc_book_asks
    idx_bgn = 0; idx_end = book_recs.length - 1;
    while (idx_bgn < idx_end)
    {
      var  price_cmp;
      idx_book  = Math.round((idx_bgn + idx_end) / 2);
      price_cmp = book_recs[idx_book].price;
      if (book_rec.price <  price_cmp) {
        idx_end = idx_book - 1;
      }
      else
      if (book_rec.price >  price_cmp) {
        idx_bgn = idx_book + 1;
      }
      else {
        idx_bgn = idx_end = idx_book;
      }
    }
    idx_book  = idx_end; // now idx_book and idx_end < book_recs.length
    // delete/add/update book record in self.loc_book_bids or self.loc_book_asks
    flag_del  = (book_rec.count == 0) ? true : false;
    if  (flag_del) {
      if ((idx_book <  0) || (idx_book >= book_recs.length)) {
        flag_del  = false;
      }
      else {
        book_recs.splice(idx_book, 1);
      }
    }
    else
    if ((idx_book <  0) || (book_rec.price >  book_recs[idx_book].price)) {
      idx_book = idx_book + 1;
      if ((idx_book <  0) || (idx_book >= book_recs.length)) {
        book_recs.push(book_rec);
      }
      else {
        book_recs.splice(idx_book, 0, book_rec);
      }
    }
    else
    if (book_rec.price <  book_recs[idx_book].price) {
      if ((idx_book <  0) || (idx_book >= book_recs.length)) {
        book_recs.push(book_rec);
      }
      else {
        book_recs.splice(idx_book, 0, book_rec);
      }
    }
    else {
      book_recs.splice(idx_book, 1, book_rec);
    }
    // update .sumamt in self.loc_book_bids or self.loc_book_asks
    var  idx_last, idx_sum;
    idx_last  =  idx_book + (flag_bids ? 1 : -1);
    idx_last  = (idx_last <  0 || idx_last >= book_recs.length) ? -1 : idx_last;
    for (idx_sum=idx_book; idx_sum >= 0 && idx_sum <  book_recs.length; idx_sum+=flag_bids ? -1 : 1)
    {
      book_recs[idx_sum].sumamt = 0.0 + book_recs[idx_sum].amount +
              ((idx_last <  0) ? 0.0 : book_recs[idx_last].sumamt);
      idx_last = idx_sum;
    }
    // invoke callback
    this.onLocRecChg_CB(flag_sece, flag_del ? book_rec : book_recs[idx_book],
                flag_bids, idx_book, flag_del);
  }

  onLocRecChg_CB(flag_sece, book_rec, flag_bids, idx_book, flag_del)
  {
  }

  // develop/debug support
  devCheck_Books(err_ses)
  {
    var  arr_errors = [];
    var  idx_rec, idx_other;
    var  strErr_pref = this.constructor.name + ' Err' +
                 ((err_ses == null || err_ses == '') ? ':' : ('(' + err_ses + '):'));
    // check book of bids
    for (idx_rec=0; idx_rec < this.loc_book_bids.length; idx_rec++)
    {
      var  sum_diff;
      idx_other = idx_rec-1;
      if ((idx_other >= 0) && (idx_other <  this.loc_book_bids.length) &&
          (this.loc_book_bids[idx_rec].price <= this.loc_book_bids[idx_other].price)) {
        arr_errors.push(strErr_pref + "(book bids disorder): "
                        + "len=" + this.loc_book_bids.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_bids[idx_rec]) +
                    ", last=" + JSON.stringify(this.loc_book_bids[idx_other]));
      }
      idx_other = idx_rec-1;
      if ((idx_other >= 0) && (idx_other <  this.loc_book_bids.length) &&
          (Math.abs(sum_diff = (this.loc_book_bids[idx_other].sumamt - this.loc_book_bids[idx_other].amount -
                        this.loc_book_bids[idx_rec].sumamt)) >  0.0001)) {
        arr_errors.push(strErr_pref + "(book bids sum mistake): "
                        + "len=" + this.loc_book_bids.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_bids[idx_rec]) +
                    ", next=" + JSON.stringify(this.loc_book_asks[idx_other]) +
                    ", diff=" + sum_diff);
      }
    }
    // check book of asks
    for (idx_rec=0; idx_rec < this.loc_book_asks.length; idx_rec++)
    {
      var  sum_diff;
      idx_other = idx_rec-1;
      if ((idx_other >= 0) && (idx_other < this.loc_book_asks.length) &&
          (this.loc_book_asks[idx_rec].price <= this.loc_book_asks[idx_other].price)) {
        arr_errors.push(strErr_pref + "(book asks disorder): "
                        + "len=" + this.loc_book_asks.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_asks[idx_rec]) +
                    ", last=" + JSON.stringify(this.loc_book_asks[idx_other]));
      }
      idx_other = idx_rec+1;
      if ((idx_other >= 0) && (idx_other < this.loc_book_asks.length) &&
          (Math.abs(sum_diff = (this.loc_book_asks[idx_other].sumamt - this.loc_book_asks[idx_other].amount -
                        this.loc_book_asks[idx_rec].sumamt)) >  0.0001)) {
        arr_errors.push(strErr_pref + "(book asks sum mistake): "
                        + "len=" + this.loc_book_asks.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_asks[idx_rec]) +
                    ", next=" + JSON.stringify(this.loc_book_asks[idx_other]) +
                    ", diff=" + sum_diff);
      }
    }
    return arr_errors;
  }
}

class ClDataSet_ACandles extends ClDataSet_Array
{
  constructor(recs_size, wreq_args)
  {
    super("candles", wreq_args);
    this.loc_recs_size   = recs_size;
    this.loc_candle_recs = [];
  }

  onLocCleanData_impl()
  {
    this.loc_candle_recs.length = 0;
  }

  onLocRecChg_impl(flag_sece, obj_rec)
  {
    var  flag_chg, rec_index;
    var  candle_rec = {
        mts:    Number(obj_rec[0]),
        open:   Number(obj_rec[1]),
        close:  Number(obj_rec[2]),
        high:   Number(obj_rec[3]),
        low:    Number(obj_rec[4]),
        volume: Number(obj_rec[5]),
      };
    flag_chg  = false;
    for (rec_index=this.loc_candle_recs.length-1; rec_index >= 0; rec_index--) {
      if (candle_rec.mts >= this.loc_candle_recs[rec_index].mts) {
        break;
      }
    }
    if ((rec_index >= 0) && (this.loc_candle_recs[rec_index].mts == candle_rec.mts))
    {
      if (this.loc_candle_recs[rec_index].volume != candle_rec.volume) {
        this.loc_candle_recs.splice(rec_index, 1, candle_rec);
        flag_chg  = true;
      }
    }
    else
    {
      if (this.loc_candle_recs.length+1 >  this.loc_recs_size) {
        this.loc_candle_recs.pop();
      }
      if ((rec_index <  0) || (candle_rec.mts >  this.loc_candle_recs[rec_index].mts)) {
        rec_index ++;
      }
      if (rec_index >= this.loc_candle_recs.length) {
        this.loc_candle_recs.push(candle_rec);
      }
      else {
        this.loc_candle_recs.splice(rec_index, 0, candle_rec);
      }
      flag_chg  = true;
    }
    if (flag_chg) {
      this.onLocRecChg_CB(flag_sece, candle_rec, rec_index);
    }
  }

  onLocRecChg_CB(flag_sece, candle_rec, rec_index)
  {
  }

  // develop/debug support
  devCheck_Candles(err_ses)
  {
    var  arr_errors = [];
    var  idx_rec, idx_other;
    var  strErr_pref = this.constructor.name + ' Err' +
                 ((err_ses == null || err_ses == '') ? ':' : ('(' + err_ses + '):'));
    // check book of bids
    for (idx_rec=0; idx_rec < this.loc_book_bids.length; idx_rec++)
    {
      var  sum_diff;
      idx_other = idx_rec-1;
      if ((idx_other >= 0) && (idx_other <  this.loc_book_bids.length) &&
          (this.loc_book_bids[idx_rec].price <= this.loc_book_bids[idx_other].price)) {
        arr_errors.push(strErr_pref + "(book bids disorder): "
                        + "len=" + this.loc_book_bids.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_bids[idx_rec]) +
                    ", last=" + JSON.stringify(this.loc_book_bids[idx_other]));
      }
      idx_other = idx_rec-1;
      if ((idx_other >= 0) && (idx_other <  this.loc_book_bids.length) &&
          (Math.abs(sum_diff = (this.loc_book_bids[idx_other].sumamt - this.loc_book_bids[idx_other].amount -
                        this.loc_book_bids[idx_rec].sumamt)) >  0.0001)) {
        arr_errors.push(strErr_pref + "(book bids sum mistake): "
                        + "len=" + this.loc_book_bids.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_bids[idx_rec]) +
                    ", next=" + JSON.stringify(this.loc_book_asks[idx_other]) +
                    ", diff=" + sum_diff);
      }
    }
    // check book of asks
    for (idx_rec=0; idx_rec < this.loc_book_asks.length; idx_rec++)
    {
      var  sum_diff;
      idx_other = idx_rec-1;
      if ((idx_other >= 0) && (idx_other < this.loc_book_asks.length) &&
          (this.loc_book_asks[idx_rec].price <= this.loc_book_asks[idx_other].price)) {
        arr_errors.push(strErr_pref + "(book asks disorder): "
                        + "len=" + this.loc_book_asks.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_asks[idx_rec]) +
                    ", last=" + JSON.stringify(this.loc_book_asks[idx_other]));
      }
      idx_other = idx_rec+1;
      if ((idx_other >= 0) && (idx_other < this.loc_book_asks.length) &&
          (Math.abs(sum_diff = (this.loc_book_asks[idx_other].sumamt - this.loc_book_asks[idx_other].amount -
                        this.loc_book_asks[idx_rec].sumamt)) >  0.0001)) {
        arr_errors.push(strErr_pref + "(book asks sum mistake): "
                        + "len=" + this.loc_book_asks.length + ",idx=" + idx_rec +
                    ", new=" + JSON.stringify(this.loc_book_asks[idx_rec]) +
                    ", next=" + JSON.stringify(this.loc_book_asks[idx_other]) +
                    ", diff=" + sum_diff);
      }
    }
    return arr_errors;
  }
}

//export { ClDataSet_ACandles, ClDataSet_ABooks, };

module.exports = {
  ClDataSet_Ticker:   ClDataSet_Ticker,
  ClDataSet_ACandles: ClDataSet_ACandles,
  ClDataSet_ABooks:   ClDataSet_ABooks,
}

