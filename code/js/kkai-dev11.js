// module kkai-dev11

const DFMT_KKAIPRIV = 1001;
const DFMT_BFXV2 = 2001;

class ClDataSet_Base
{
  constructor(name_chan, wreq_args)
  {
    this.name_chan = name_chan;
    this.id_chan = null;
    this.wreq_args = wreq_args;
    this.flag_sys_time  = false;
    this.loc_time_this  = 0;
  }

  locSet_ChanId(id_chan)
  {
    this.id_chan = id_chan;
  }

  locDataClean()
  {
    this.onLocDataClean_impl();
    this.onLocDataClean_CB();
  }
  locDataSync()
  {
    this.onLocDataSync_impl();
    this.onLocDataSync_CB();
  }

  locDataAppend(fmt_data, obj_msg)
  {
    if (this.flag_sys_time) {
      this.loc_time_this = Date.now();
    }
    this.onLocDataAppend_impl(fmt_data, obj_msg);
    this.onLocDataAppend_CB(null);
  }

  locRecAdd(flag_plus, fmt_data, obj_rec)
  {
    this.onLocRecAdd_impl(flag_plus, fmt_data, obj_rec)
  }

  onLocDataClean_CB()
  {
  }
  onLocDataAppend_CB(chan_data)
  {
  }

  onLocDataClean_impl()
  {
  }
  onLocDataSync_impl()
  {
  }
  onLocDataSync_CB()
  {
  }

  onLocDataAppend_impl(fmt_data, obj_msg)
  {
  }

  onLocRecAdd_impl(flag_plus, fmt_data, obj_rec)
  {
  }
}

class ClDataSet_Array extends ClDataSet_Base
{
  constructor(name_chan, wreq_args)
  {
    super(name_chan, wreq_args);
  }

  onLocDataAppend_impl(fmt_data, obj_msg)
  {
    var data_msg = null;

    if (fmt_data == DFMT_KKAIPRIV) {
      data_msg  = obj_msg['data'];
    }
    else
	if (fmt_data == DFMT_BFXV2)
    {
      data_msg  = obj_msg[1];
    }

    if (!Array.isArray(data_msg))
    {
    }
    else
    if (!Array.isArray(data_msg[0]))
    {
      this.locRecAdd(true, fmt_data, data_msg);
    }
    else
    {
      var  idx_rec, num_rec;
      num_rec = data_msg.length;
      this.locDataClean(data_msg);
      for (idx_rec=0; idx_rec <  num_rec; idx_rec++) {
        this.locRecAdd(false, fmt_data, data_msg[idx_rec]);
      }
      this.locDataSync();
    }
  }
}

class ClDataSet_Ticker extends ClDataSet_Base
{
  constructor(wreq_args)
  {
    super('ticker', wreq_args);
  }

  onLocDataAppend_impl(fmt_data, obj_msg)
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
      this.onLocRecAdd_CB(ticker_rec, 0);
    }
  }

  onLocRecAdd_CB(ticker_rec, rec_index)
  {
  }
}

class ClDataSet_ABooks extends ClDataSet_Array
{
  constructor(wreq_args)
  {
    super("book", wreq_args);
    this.loc_book_bids  = [];
    this.loc_book_asks  = [];
  }

  onLocDataClean_impl()
  {
    this.loc_book_bids.length = 0;
    this.loc_book_asks.length = 0;
  }

  onLocRecAdd_impl(flag_plus, fmt_data, obj_rec)
  {
    var  flag_bids;
    var  book_rec;
    var  idx_book, idx_bgn, idx_end;
    var  flag_del;
    var  book_recs;
    if (fmt_data == DFMT_KKAIPRIV)
    {
      flag_bids  = (obj_rec['type'] == 'bid') ? true : false;
      book_rec   = obj_rec;
    }
    else
	if (fmt_data == DFMT_BFXV2)
    {
      var  amount_rec;
      flag_bids  = (obj_rec[2] >  0.0) ? true : false;
      amount_rec = Number(flag_bids ? obj_rec[2] : (0.0 - obj_rec[2]));
      book_rec   = {
        price:  Number(obj_rec[0]),
        type:   flag_bids ? 'bid' : 'ask',
        count:  obj_rec[1],
        amount: amount_rec,
        sumamt: 0.0,
      };
    }
    book_recs = flag_bids ? this.loc_book_bids : this.loc_book_asks;
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
    this.onLocRecAdd_CB(flag_plus, flag_del ? book_rec : book_recs[idx_book],
                flag_bids, idx_book, flag_del);
  }

  onLocRecAdd_CB(flag_plus, book_rec, flag_bids, idx_book, flag_del)
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

  onLocDataClean_impl()
  {
    this.loc_candle_recs.length = 0;
  }

  onLocRecAdd_impl(flag_plus, fmt_data, obj_rec)
  {
    var  flag_chg, rec_index;
    var  candle_rec;
    if (fmt_data == DFMT_KKAIPRIV)
    {
      candle_rec = obj_rec;
    }
    else
	if (fmt_data == DFMT_BFXV2)
    {
      candle_rec = {
        mts:    Number(obj_rec[0]),
        open:   Number(obj_rec[1]),
        close:  Number(obj_rec[2]),
        high:   Number(obj_rec[3]),
        low:    Number(obj_rec[4]),
        volume: Number(obj_rec[5]),
      };
    }
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
      this.onLocRecAdd_CB(flag_plus, candle_rec, rec_index);
    }
  }

  onLocRecAdd_CB(flag_plus, candle_rec, rec_index)
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

