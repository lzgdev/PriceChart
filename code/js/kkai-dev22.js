

// AnyChart sample code: javascript
var  cl1Buy,  cl2Buy;
var cl1Sell, cl2Sell;

cl1Buy  = "#228B22";
cl2Buy  = "#2E8B57";
cl1Sell = "#8B0000";
cl2Sell = "#DC143C";

cl1Buy  = '#00FF00'; // Lime
cl2Buy  = '#32CD32'; // LimeGreen
cl1Sell = '#FF4500'; // OrangeRed
//cl2Sell = '#C71585'; // MediumVioletRed
cl2Sell = '#F08080'; // LightCoral

var num_change = 0;

class ClChanData_ABooks_HighCharts extends ClChanData_ABooks
{
  constructor(prec, len)
  {
    super(prec, len)
    this.loc_num_bids = 0;
    this.loc_num_asks = 0;
    this.loc_highcharts_numcol = Math.round(this.req_book_len + this.req_book_len / 3);
    // index between bids and asks
    this.loc_highcharts_needel = 0;
    this.loc_highcharts_amount = [];
    this.loc_highcharts_sumamt = [];
    this.loc_price_unit =  1.0;
    this.loc_price_min  =  0.0;
    this.loc_price_max  = -1.0;
    if (this.req_book_prec == 'P0') {
      this.loc_price_unit =   0.1;
    }
    else
    if (this.req_book_prec == 'P1') {
      this.loc_price_unit =   1.0;
    }
    else
    if (this.req_book_prec == 'P2') {
      this.loc_price_unit =  10.0;
    }
    else
    if (this.req_book_prec == 'P3') {
      this.loc_price_unit = 100.0;
    }
  }

  onLocCleanData_CB()
  {
    this.loc_num_bids = 0;
    this.loc_num_asks = 0;
    this.loc_highcharts_needel = 0;
    this.loc_highcharts_amount.length = 0;
    this.loc_highcharts_sumamt.length = 0;
  }

  onLocBookChg_CB(book_rec, flag_bids, idx_book, flag_del)
  {
    var  sers_data_bids = [], sers_data_asks = [];

    var  idx_this, idx_pair;
    for (idx_this=0; idx_this <  this.loc_book_bids.length; idx_this++)
    {
      var  pric_this;
      idx_pair  =  idx_this - 1;
      pric_this = (idx_pair >= 0) ? this.loc_book_bids[idx_pair].price :
                        (this.loc_book_bids[idx_this].price - this.loc_price_unit);
      for (pric_this += this.loc_price_unit;
           pric_this <  this.loc_book_bids[idx_this].price;
           pric_this += this.loc_price_unit)
      {
        sers_data_bids.push({ x: pric_this, y: this.loc_book_bids[idx_this].sumamt, });
      }
      sers_data_bids.push({ x: pric_this, y: this.loc_book_bids[idx_this].sumamt, });
    }
    for (idx_this=0; idx_this <  this.loc_book_asks.length; idx_this++)
    {
      var  pric_this;
      idx_pair  =  idx_this - 1;
      pric_this = (idx_pair >= 0) ? this.loc_book_asks[idx_pair].price :
                 ((this.loc_book_bids.length == 0) ? (this.loc_book_asks[idx_this].price - this.loc_price_unit) :
                                                     (this.loc_book_bids[this.loc_book_bids.length-1].price));
      for (pric_this += this.loc_price_unit;
           pric_this <  this.loc_book_asks[idx_this].price;
           pric_this += this.loc_price_unit)
      {
        sers_data_asks.push({ x: pric_this, y: ((idx_pair < 0) ? 0.0 : this.loc_book_asks[idx_pair].sumamt), });
      }
      sers_data_asks.push({ x: pric_this, y: this.loc_book_asks[idx_this].sumamt, });
    }

    highcharts_chart.series[0].setData(sers_data_bids);
    highcharts_chart.series[1].setData(sers_data_asks);

    if ((num_change % 2) == 0) {
      highcharts_chart.redraw({});
    }
  }

  onLocRecChg_CB(obj_rec, idx_rec, flag_del, flag_new, flag_up)
  {
    var idx_all;
    var amount_last, amtsum_last;
    var price_rec =  obj_rec[0];
    var flag_bids = (obj_rec[2] > 0.0) ? true : false;
    var amount_rec = flag_bids ? obj_rec[2] : (0.0 - obj_rec[2]);
    idx_all  = idx_rec + (flag_bids ? 0 : this.loc_num_bids);
//$('#log_out2').append("\nchar data: idx=" + idx_rec + ", price=" + price_rec + ", del=" + flag_del + ", new=" + flag_new + ", up=" + flag_up);
    // operate(del/new/up) on this.loc_highcharts_amount and this.loc_highcharts_sumamt
    if (flag_del) {
      this.loc_highcharts_amount.kk_delete_at(idx_all);
      this.loc_highcharts_sumamt.kk_delete_at(idx_all);
      this.loc_num_bids -= flag_bids ? 1 : 0;
      this.loc_num_asks -= flag_bids ? 0 : 1;
    }
    else
    if (flag_new) {
      amount_last  = 0.0;
      amtsum_last  = 0.0;
      if ( flag_bids && idx_all   <  this.loc_num_bids) {
        amount_last  = this.loc_highcharts_amount[idx_all];
        amtsum_last  = this.loc_highcharts_sumamt[idx_all];
      }
      else
      if (!flag_bids && idx_all-1 >= this.loc_num_bids) {
        amount_last  = this.loc_highcharts_amount[idx_all-1];
        amtsum_last  = this.loc_highcharts_sumamt[idx_all-1];
      }
      this.loc_highcharts_amount.kk_insert_at(idx_all, amount_rec);
      this.loc_highcharts_sumamt.kk_insert_at(idx_all, amtsum_last + amount_last);
      this.loc_num_bids += flag_bids ? 1 : 0;
      this.loc_num_asks += flag_bids ? 0 : 1;
    }
    else
    if (flag_up)  {
      if ( flag_bids && idx_all+1 <  this.loc_num_bids) {
        amount_last  = this.loc_highcharts_amount[idx_all+1];
        amtsum_last  = this.loc_highcharts_sumamt[idx_all+1];
      }
      else
      if (!flag_bids && idx_all-1 >= this.loc_num_bids) {
        amount_last  = this.loc_highcharts_amount[idx_all-1];
        amtsum_last  = this.loc_highcharts_sumamt[idx_all-1];
      }
      this.loc_highcharts_amount[idx_all]  = amount_rec;
      this.loc_highcharts_sumamt[idx_all]  = amount_last + amtsum_last;
    }
    // update amount of each this.loc_highcharts_amount and this.loc_highcharts_sumamt
    var count_rows, idx_last = -1;
    idx_all += (!flag_del) ? 0 : (flag_bids ? -1 : 0);
    if ( flag_bids && idx_all+1 <  this.loc_num_bids) {
      idx_last = idx_all + 1;
    }
    else
    if (!flag_bids && idx_all-1 >= this.loc_num_bids) {
      idx_last = idx_all - 1;
    }
    count_rows = this.loc_num_bids + this.loc_num_asks;
    while (( flag_bids && idx_all >= 0) || (!flag_bids && idx_all <  count_rows))
    {
      this.loc_highcharts_sumamt[idx_all] = (idx_last < 0) ? 0.0 : 
                            0.0 + this.loc_highcharts_amount[idx_last] + this.loc_highcharts_sumamt[idx_last];
      idx_last = idx_all;
      idx_all += flag_bids ? -1 :  1;
    }

/*
var out_msg;
out_msg = 'amount: ' + JSON.stringify(this.loc_highcharts_amount);
console.log(out_msg);
//$('#log_out2').append('\n' + out_msg);
out_msg = 'amtsum: ' + JSON.stringify(this.loc_highcharts_sumamt);
console.log(out_msg);
//$('#log_out2').append('\n' + out_msg);
// */
/*
if (++num_change < 50)
{
highcharts_chart.update({
    series: [
      {
        data: this.loc_highcharts_sumamt,
      },
      {
        data: this.loc_highcharts_amount,
      },
    ],
    });
}
// */
/*
highcharts_chart.series[0].setData(this.loc_highcharts_amount, false);
highcharts_chart.series[1].setData(this.loc_highcharts_sumamt, false);
highcharts_chart.redraw({});
// */
/*
if (++num_change < 50)
{
highcharts_chart.series[0].setData(this.loc_highcharts_amount, false);
highcharts_chart.series[1].setData(this.loc_highcharts_sumamt, false);
highcharts_chart.redraw({});
}
// */
/*
if (++num_change < 50)
{
var out_msg;
var out_array;
out_array = [];
for (var i=0; i < this.loc_highcharts_amount.length; i++) {
  out_array.push(Math.round(this.loc_highcharts_amount[i]));
}
out_msg = 'out(amount): ' + JSON.stringify(out_array) + ', amount: ' + JSON.stringify(this.loc_highcharts_amount);
console.log(out_msg);
//$('#log_out2').append('\n' + out_msg);
highcharts_chart.series[0].setData(out_array, false);
out_array = [];
for (var i=0; i < this.loc_highcharts_amount.length; i++) {
  out_array.push(Math.round(this.loc_highcharts_sumamt[i]));
}
out_msg = 'out(amtsum): ' + JSON.stringify(out_array) + ', amtsum: ' + JSON.stringify(this.loc_highcharts_sumamt);
console.log(out_msg);
//$('#log_out2').append('\n' + out_msg);
highcharts_chart.series[1].setData(out_array, false);
highcharts_chart.redraw({});
}
// */

    // TEMP: temp develop/debug code
    /*
    var err_ses = null;
    var arr_errors = this.devCheck_Books(err_ses);
    for (idx_all = 0; idx_all < arr_errors.length; idx_all++) {
      $('#log_out2').append('\n' + arr_errors[idx_all]);
    }
    // */
  }
}

