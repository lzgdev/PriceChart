
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
    var  flag_bids  = (obj_rec[2] >  0.0) ? true : false;
    var  amount_rec =  Number(flag_bids ? obj_rec[2] : (0.0 - obj_rec[2]));
    var  book_rec = {
        price:  Number(obj_rec[0]),
        count:  Number(obj_rec[1]),
        amount: amount_rec,
        sumamt: Number(0.0),
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
      if (idx_book <  0) {
        flag_del  = false;
      }
      else {
        book_recs.kk_delete_at(idx_book);
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
    //
    this.onLocBookChg_CB(flag_del ? book_rec : book_recs[idx_book], flag_bids, idx_book, flag_del);
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

