
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
    this.req_book_prec  = String(prec);
    this.req_book_len   = Number(len);
    this.loc_book_bids  = [];
    this.loc_book_asks  = [];
  }

  onLocCleanData_impl()
  {
    this.loc_book_bids.length = 0;
    this.loc_book_asks.length = 0;
  }

  onLocRecChg_impl(obj_rec)
  {
    // locate the book record from self.loc_book_bids or self.loc_book_asks
    var  book_rec = {
        price:  Number(obj_rec[0]),
        count:  Number(obj_rec[1]),
        amount: Number((obj_rec[2] >  0.0) ? obj_rec[2] : (0.0 - obj_rec[2])),
      };
    var  idx_book, idx_bgn, idx_end;
    var  flag_del;
    var  flag_bids = (book_rec.amount >  0.0) ? true : false;
    var  book_recs = flag_bids ? this.loc_book_bids : this.loc_book_asks;
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
      if (idx_book >= 0) {
        book_recs.kk_delete_at(idx_book);
      }
      else {
        flag_del  = false;
      }
    }
    else
    if ((idx_book <  0) || (book_rec.price >  book_recs[idx_book].price)) {
      idx_book = idx_book + 1;
      book_recs.kk_insert_at(idx_book, book_rec);
    }
    else
    if (book_rec.price <  book_recs[idx_book].price) {
      book_recs.kk_insert_at(idx_book, book_rec);
    }
    else {
      book_recs.kk_update_at(idx_book, book_rec);
    }
    //
    this.onLocBookChg_CB(book_rec, flag_bids, idx_book, flag_del);
  }

  onLocBookChg_CB(book_rec, flag_bids, idx_book, flag_del)
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

