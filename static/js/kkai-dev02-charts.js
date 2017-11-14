
// AnyChart sample code: javascript
var  cl1Buy = "#228B22",  cl2Buy = "#2E8B57";
var cl1Sell = "#8B0000", cl2Sell = "#DC143C";

class ClChanData_ABooks_AnyChart extends ClChanData_ABooks
{
  constructor(prec)
  {
    super(prec)
    this.loc_num_bids = 0;
    this.loc_num_asks = 0;
    this.loc_anychart_datalist = [];
    this.loc_anychart_dataset  = anychart.data.set([ ]);
  }

  onLocCleanData_CB()
  {
    this.loc_anychart_datalist = [];
    while (this.loc_anychart_dataset.getRowsCount() > 0) {
      this.loc_anychart_dataset.remove(0);
    }
    this.loc_num_bids = 0;
    this.loc_num_asks = 0;
  }

  onLocRecChg_CB(obj_rec, idx_rec, flag_del, flag_new, flag_up)
  {
    var idx_list;
    var price_rec =  obj_rec[0];
    var flag_bids = (obj_rec[2] > 0.0) ? true : false;
    var amount_rec = flag_bids ? obj_rec[2] : (0.0 - obj_rec[2]);
    idx_list = idx_rec + (flag_bids ? 0 : this.loc_num_bids);
    idx_rec += flag_bids ? 0 : this.loc_num_bids;
    //console.log("char data: idx=" + idx_rec + ", price=" + price_rec + ", del=" + flag_del + ", new=" + flag_new + ", up=" + flag_up);
    // operate(del/new/up) on this.loc_anychart_dataset
    if (flag_del) {
this.loc_anychart_datalist = [];
      this.loc_anychart_dataset.remove(idx_rec);
      this.loc_num_bids -= flag_bids ? 1 : 0;
      this.loc_num_asks -= flag_bids ? 0 : 1;
    }
    else
    if (flag_new) {
      this.loc_anychart_dataset.insert([ price_rec,
            flag_bids ? cl1Buy : cl1Sell, flag_bids ? cl2Buy : cl2Sell,
            0, amount_rec, 0.0, ], idx_rec);
      this.loc_num_bids += flag_bids ? 1 : 0;
      this.loc_num_asks += flag_bids ? 0 : 1;
    }
    else
    if (flag_up)  {
      this.loc_anychart_dataset.row(idx_rec, [ price_rec, 
            flag_bids ? cl1Buy : cl1Sell, flag_bids ? cl2Buy : cl2Sell,
            0, amount_rec, 0.0, ]);
    }
    //
    var count_rows, idx_last = -1;
    var obj_last;
    if ( flag_bids && idx_rec+1 <  this.loc_num_bids) {
      idx_last = idx_rec + 1;
    }
    else
    if (!flag_bids && idx_rec-1 >= this.loc_num_bids) {
      idx_last = idx_rec - 1;
    }
    // update amount of each this.loc_anychart_dataset.row
    count_rows = this.loc_anychart_dataset.getRowsCount();
    while (( flag_bids && idx_rec >= 0) || (!flag_bids && idx_rec <  count_rows))
    {
      obj_last = (idx_last < 0) ? null : this.loc_anychart_dataset.row(idx_last);
      obj_rec  = this.loc_anychart_dataset.row(idx_rec);
      obj_rec[5] = (obj_last == null) ? 0.0 : (obj_last[5] + obj_last[4]);
      idx_last = idx_rec;
      idx_rec += flag_bids ? -1 :  1;
    }

    // TEMP: temp develop/debug code
    /*
    var err_ses = null;
    var arr_errors = this.devCheck_Books(err_ses)
                    .concat(this.devCheck_ChartData(err_ses));
    for (idx_rec = 0; idx_rec < arr_errors.length; idx_rec++) {
      $('#log_out2').append('\n' + arr_errors[idx_rec]);
    }
    // */
  }

  // develop/debug support
  devCheck_ChartData(err_ses)
  {
    var  num_shift;
    var  idx_rec, idx_other;
    var  strErr_pref = this.constructor.name + ' Err' +
                 ((err_ses == null || err_ses == '') ? ':' : ('(' + err_ses + '):'));
    var arr_errors = [];
    //
    if ((this.loc_num_bids + this.loc_num_asks) !=
                    this.loc_anychart_dataset.getRowsCount()) {
        arr_errors.push(strErr_pref + "(books num): num_data=" + this.loc_anychart_dataset.getRowsCount()
                        + ", num_bids=" + this.loc_num_bids + ", num_asks=" + this.loc_num_asks);
    }
    if (this.loc_num_bids != this.loc_book_bids.length) {
        arr_errors.push(strErr_pref + "(book bids): num_loc=" + this.loc_book_bids.length
                        + ", num_chart=" + this.loc_num_bids);
    }
    if (this.loc_num_asks != this.loc_book_asks.length) {
        arr_errors.push(strErr_pref + "(book asks): num_loc=" + this.loc_book_asks.length
                        + ", num_chart=" + this.loc_num_asks);
    }
    num_shift = 0;
    for (idx_rec=0; idx_rec < this.loc_book_bids.length; idx_rec++)
    {
      var row_this, row_other;
      idx_other = idx_rec + 1;
      row_this  = this.loc_anychart_dataset.row(num_shift + idx_rec);
      if (this.loc_book_bids[idx_rec][0] != row_this[0]) {
          arr_errors.push(strErr_pref + "(book bid price): idx=" + idx_rec + ", " +
                    "rec=" + JSON.stringify(this.loc_book_bids[idx_rec]) + ", " +
                    "row=" + JSON.stringify(row_this));
      }
      if (idx_other < this.loc_book_bids.length)
      {
        row_other = this.loc_anychart_dataset.row(num_shift + idx_other);
        if (row_this[5] != (row_other[5] + row_other[4])) {
          arr_errors.push(strErr_pref + "(book bids sum): idx=" + idx_rec + ", " +
                    "this=" + JSON.stringify(row_this) + ", " +
                    "other=" + JSON.stringify(row_other));
        }
      }
    }
    num_shift = this.loc_num_bids;
    for (idx_rec=0; idx_rec < this.loc_book_asks.length; idx_rec++)
    {
      var row_this, row_other;
      idx_other = idx_rec - 1;
      row_this  = this.loc_anychart_dataset.row(num_shift + idx_rec);
      if (this.loc_book_asks[idx_rec][0] != row_this[0]) {
          arr_errors.push(strErr_pref + "(book ask price): idx=" + idx_rec + ", " +
                    "rec=" + JSON.stringify(this.loc_book_asks[idx_rec]) + ", " +
                    "row=" + JSON.stringify(row_this));
      }
      if (idx_other >= 0)
      {
        row_other = this.loc_anychart_dataset.row(num_shift + idx_other);
        if (row_this[5] != (row_other[5] + row_other[4])) {
          arr_errors.push(strErr_pref + "(book asks sum): idx=" + idx_rec + ", " +
                    "this=" + JSON.stringify(row_this) + ", " +
                    "other=" + JSON.stringify(row_other));
        }
      }
    }
    return arr_errors;
  }
}

