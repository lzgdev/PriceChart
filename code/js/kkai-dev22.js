

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
    var  srs_books;
    srs_books = highcharts_chart.series[0];
    if (this.loc_price_max <  this.loc_price_min) {
      if (flag_del) {
        return;
      }
      srs_books.addPoint([book_rec.price, book_rec.amount], false);
      this.loc_price_min = this.loc_price_max = book_rec.price;
      return;
    }
    if (flag_del)
    {
//      srs_books.addPoint([this.loc_price_min, 0.0], false);
    }
    else
    if (book_rec.price <  this.loc_price_min)
    {
      this.loc_price_min -= this.loc_price_unit;
      for (; book_rec.price <  this.loc_price_min; this.loc_price_min-=this.loc_price_unit)
      {
        srs_books.addPoint([this.loc_price_min, book_rec.amount], false);
      }
      srs_books.addPoint([this.loc_price_min, flag_bids ? book_rec.amount : 0.0], false);
    }
    else
    if (book_rec.price >  this.loc_price_max)
    {
      this.loc_price_max += this.loc_price_unit;
      for (; book_rec.price >  this.loc_price_max; this.loc_price_max += this.loc_price_unit)
      {
        srs_books.addPoint([this.loc_price_max, book_rec.amount], false);
      }
      srs_books.addPoint([this.loc_price_max, flag_bids ? book_rec.amount : 0.0], false);
    }
    else
    {
      var  book_recs = flag_bids ? this.loc_book_bids : this.loc_book_asks;
      while ((flag_bids && idx_book >= 0) || (!flag_bids && idx_book <  book_recs.length))
      {
        var  idx_sers, prc_sers, prc_next;
        book_rec  = book_recs[idx_book];
        prc_sers  = book_rec.price;
        prc_next  = (flag_bids) ? ((idx_book-1 <  0) ? 0.0 : book_recs[idx_book-1].price) :
                                  ((idx_book+1 >= book_recs.length) ? (prc_sers+200) : book_recs[idx_book+1].price);
        while ((flag_bids && prc_sers >  prc_next) || (!flag_bids && prc_sers <  prc_next))
        {
          idx_sers  = Math.round((prc_sers - this.loc_price_min) / this.loc_price_unit);
          if (idx_sers <  0 || idx_sers >= srs_books.data.length) {
            break;
          }
          srs_books.data[idx_sers].update({ x: prc_sers, y: book_rec.amount, }, false);
          prc_sers += flag_bids ? (0-this.loc_price_unit) : this.loc_price_unit;
        }
        idx_book += (flag_bids) ? -1 :  1;
      }
    }

if ((++num_change % 10) == 0)
{
  highcharts_chart.redraw({});
}
/*
$('#log_out2').append("\nbook bids=" + JSON.stringify(this.loc_book_bids));
$('#log_out2').append("\nbook asks=" + JSON.stringify(this.loc_book_asks));
 */
/*
    this.loc_book_bids  = [];
    this.loc_book_asks  = [];
// */
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

