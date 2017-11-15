
class ClChanData
{
  constructor()
  {
    this.chan_id = null;
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
    this.onLocAppendData_impl(obj_msg);
  }

  locRecChg(obj_rec)
  {
    this.onLocRecChg_impl(obj_rec)
  }

  onLocCleanData_CB()
  {
  }

  onLocRecChg_CB(obj_rec, idx_rec, flag_del, flag_new, flag_up)
  {
  }

  onLocCleanData_impl()
  {
  }

  onLocAppendData_impl(obj_msg)
  {
  }

  onLocRecChg_impl(obj_rec)
  {
  }
}

class ClChanData_Array extends ClChanData
{
  constructor()
  {
    super();
  }

  onLocAppendData_impl(obj_msg)
  {
    if (!Array.isArray(obj_msg[1]))
    {
    }
    else
    {
      var book_msg = obj_msg[1];
      if (!Array.isArray(book_msg[0]))
      {
        this.locRecChg(book_msg);
      }
      else
      {
        this.locCleanData(book_msg);
        for (var i=0; i < book_msg.length; i++) {
          this.locRecChg(book_msg[i]);
        }
      }
    }
  }
}

class ClChanData_ABooks extends ClChanData_Array
{
  constructor(prec, len)
  {
    super();
    this.req_book_prec = prec;
    this.req_book_len  = len;
    this.loc_book_bids = [];
    this.loc_book_asks = [];
  }

  onLocCleanData_impl()
  {
    this.loc_book_bids = [];
    this.loc_book_asks = [];
  }

  onLocRecChg_impl(obj_rec)
  {
    // locate the book record from self.loc_book_bids or self.loc_book_asks
    var price_rec = obj_rec[0];
    var flag_bids = (obj_rec[2] > 0.0) ? true : false;
    var book_recs = flag_bids ? this.loc_book_bids : this.loc_book_asks;
    var idx_rec, idx_bgn, idx_end;
    var flag_del, flag_new, flag_up;
    idx_bgn = 0; idx_end = book_recs.length - 1;
    while (idx_bgn < idx_end)
    {
      var price_cmp;
      idx_rec = Math.round((idx_bgn + idx_end) / 2);
      price_cmp = book_recs[idx_rec][0];
      if (price_rec <  price_cmp) {
        idx_end = idx_rec - 1;
      }
      else
      if (price_rec >  price_cmp) {
        idx_bgn = idx_rec + 1;
      }
      else {
        idx_bgn = idx_end = idx_rec;
      }
    }
    idx_rec = idx_end;
    // delete/add/update book record in self.loc_book_bids or self.loc_book_asks
    flag_del = flag_new = flag_up  = false;
    if  (idx_rec >= 0 && obj_rec[1] == 0) {
      flag_del = true;
      if (price_rec == book_recs[idx_rec][0]) {
        book_recs.kk_delete_at(idx_rec);
      }
    }
    else
    if (idx_rec <  0  || price_rec > book_recs[idx_rec][0]) {
      flag_new = true;
      idx_rec = idx_rec + 1;
      book_recs.kk_insert_at(idx_rec, obj_rec);
    }
    else
    if (price_rec < book_recs[idx_rec][0]) {
      flag_new = true;
      book_recs.kk_insert_at(idx_rec, obj_rec);
    }
    else {
      flag_up  = true;
      book_recs.kk_update_at(idx_rec, obj_rec);
    }
    this.onLocRecChg_CB(obj_rec, idx_rec, flag_del, flag_new, flag_up);
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
      if (((idx_other = idx_rec-1) >= 0) &&
          (this.loc_book_bids[idx_rec][0] <= this.loc_book_bids[idx_other][0])) {
        arr_errors.push(strErr_pref + "(book bids disorder): "
                        + "len=" + this.loc_book_bids.length + ",idx=" + idx_rec +
                    "last=" + JSON.stringify(this.loc_book_bids[idx_other]) + ", " +
                    "new=" + JSON.stringify(this.loc_book_bids[idx_rec]));
      }
    }
    // check book of asks
    for (idx_rec=0; idx_rec < this.loc_book_asks.length; idx_rec++)
    {
      if (((idx_other = idx_rec-1) >= 0) &&
          (this.loc_book_asks[idx_rec][0] <= this.loc_book_asks[idx_other][0])) {
        arr_errors.push(strErr_pref + "(book asks disorder): "
                        + "len=" + this.loc_book_asks.length + ",idx=" + idx_rec +
                    "last=" + JSON.stringify(this.loc_book_asks[idx_other]) + ", " +
                    "new=" + JSON.stringify(this.loc_book_asks[idx_rec]));
      }
    }
    return arr_errors;
  }
}

