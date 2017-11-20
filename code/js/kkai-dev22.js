
// HighCharts sample code: javascript

class ClChanData_ABooks_HighCharts extends ClChanData_ABooks
{
  constructor(wreq_prec, wreq_len, gui_chart)
  {
    super(wreq_prec, wreq_len)
this.num_change = 0;
    this.loc_gui_chart = gui_chart;
    this.loc_need_sync = false;
    this.loc_num_bids = 0;
    this.loc_num_asks = 0;
    this.loc_highcharts_numcol = Math.round(this.req_book_len + this.req_book_len / 3);
    // index between bids and asks
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
  }

  onLocAppendData_CB()
  {
    if (!this.loc_need_sync) {
      return 0;
    }
    this.loc_need_sync = true;
this.num_change ++;
if ((this.num_change % 4) != 0) { return -1; }
    var  sers_data_bids = [], sers_data_pads = [], sers_data_asks = [];

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

    this.loc_gui_chart.series[0].setData(sers_data_bids);
    this.loc_gui_chart.series[1].setData(sers_data_pads);
    this.loc_gui_chart.series[2].setData(sers_data_asks);

    this.loc_gui_chart.redraw({});
  }

  onLocBookChg_CB(book_rec, flag_bids, idx_book, flag_del)
  {
    this.loc_need_sync = true;
  }
    // TEMP: temp develop/debug code
    /*
    var err_ses = null;
    var arr_errors = this.devCheck_Books(err_ses);
    for (idx_all = 0; idx_all < arr_errors.length; idx_all++) {
      $('#log_out2').append('\n' + arr_errors[idx_all]);
    }
    // */
}

