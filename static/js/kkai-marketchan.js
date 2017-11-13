
class ClChanData
{
  constructor(chan_id)
  {
    this.chan_id = chan_id;
  }

  locCleanData()
  {
    this.onLocCleanData_impl();
  }

  locAppendData(obj_msg)
  {
    this.onLocAppendData_impl(obj_msg);
  }

  locRecChg(obj_rec)
  {
    this.onLocRecChg_impl(obj_rec)
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
  constructor(chan_id)
  {
    super(chan_id)
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
  constructor(chan_id, prec)
  {
    super(chan_id)
    this.prec = prec;
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
    var price_rec = obj_rec[2];
    var flag_bids = (price_rec > 0.0) ? true : false;
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
      if (book_recs[idx_rec][0] == price_rec) {
        book_recs.splice(idx_rec, 1);
      }
    }
    else
    if (idx_rec <  0  || price_rec > book_recs[idx_rec][0]) {
      flag_new = true;
      idx_rec = idx_rec + 1;
      book_recs.splice(idx_rec, 0, obj_rec);
    }
    else
    if (price_rec < book_recs[idx_rec][0]) {
      flag_new = true;
      book_recs.splice(idx_rec, 0, obj_rec)
    }
    else {
      flag_up  = true;
      book_recs.splice(idx_rec, 1, obj_rec)
    }
    this.onLocRecChg_CB(obj_rec, idx_rec, flag_del, flag_new, flag_up);
  }
}

