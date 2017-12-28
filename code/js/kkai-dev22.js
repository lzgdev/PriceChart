// module kkai-dev22

class ClDataSet_ABooks_HighCharts extends ClDataSet_ABooks
{
  constructor(gui_chart, wreq_args)
  {
    super(wreq_args);
    this.loc_gui_chart = gui_chart;
    this.loc_sync_flag = false;
    this.loc_min_xaxis =  0.0;
    this.loc_max_xaxis = -1.0;
    this.loc_min_yaxis =  0.0;
    this.loc_max_yaxis = -1.0;
this.num_change = 0;
  }

  onSyncDataGUI_impl()
  {
    var  idx_book, idx_sers;
    for (idx_sers=0; idx_sers < 3; idx_sers++)
    {
      var  loc_book = null;
      var  pnts_new = []
      if (idx_sers == 0) {
        loc_book  = this.loc_book_bids;
      }
      else
      if (idx_sers == 2) {
        loc_book  = this.loc_book_asks;
      }
      if (loc_book != null) {
        for (idx_book=0; idx_book <  loc_book.length; idx_book++) {
          pnts_new.push({ x: loc_book[idx_book].price, y: loc_book[idx_book].sumamt, });
        }
      }
      else
      if (this.loc_book_bids.length >  0 && this.loc_book_asks.length >  0) {
        var  rec_bid, rec_ask;
        rec_bid = this.loc_book_bids[this.loc_book_bids.length-1]
        rec_ask = this.loc_book_asks[0]
        pnts_new.push({ x: ((rec_bid.price + rec_ask.price) / 2), y: (rec_bid.sumamt + rec_ask.sumamt), })
      }
      this.loc_gui_chart.series[idx_sers].setData(pnts_new, false);
    }

    if (this.loc_book_bids.length >  0 && this.loc_book_asks.length >  0)
    {
      var  flag_axis;
      var  axis_min, axis_max, axis_pad;
      axis_min  =  this.loc_book_bids[0].price;
      axis_max  =  this.loc_book_asks[this.loc_book_asks.length - 1].price;
      axis_pad  = (axis_max - axis_min) / 10;
      flag_axis = false;
      if (axis_min <  this.loc_min_xaxis || axis_min >= this.loc_min_xaxis+axis_pad) {
        flag_axis = true;
      }
      if (axis_max >  this.loc_max_xaxis || axis_max <= this.loc_max_xaxis-axis_pad) {
        flag_axis = true;
      }
      if (flag_axis) {
        this.loc_min_xaxis = axis_min - axis_pad/2;
        this.loc_max_xaxis = axis_max + axis_pad/2;
        this.loc_gui_chart.xAxis[0].setExtremes(this.loc_min_xaxis, this.loc_max_xaxis, false);
      }
      axis_min  =  0.0;
      axis_max  = (this.loc_book_bids[0].sumamt >= this.loc_book_asks[this.loc_book_asks.length - 1].sumamt) ?
                   this.loc_book_bids[0].sumamt : this.loc_book_asks[this.loc_book_asks.length - 1].sumamt;
      axis_pad  = (axis_max - axis_min) /  3;
      flag_axis = false;
      if (axis_max >  this.loc_max_yaxis || axis_max <= this.loc_max_yaxis-axis_pad) {
        flag_axis = true;
      }
      if (flag_axis) {
        this.loc_max_yaxis = axis_max + axis_pad/2;
        this.loc_gui_chart.yAxis[0].setExtremes(this.loc_min_yaxis, this.loc_max_yaxis, false);
      }
    }
    this.loc_gui_chart.redraw({});
  }

  onLocDataClean_CB()
  {
  }

  onLocDataAppend_CB(chan_data)
  {
    if (!this.loc_sync_flag) {
      return 0;
    }
this.num_change ++;
if ((this.num_change % 4) != 0) { return -1; }
    this.onSyncDataGUI_impl();
    this.loc_sync_flag = false;
  }

  onLocRecAdd_CB(flag_sece, book_rec, flag_bids, idx_book, flag_del)
  {
    this.loc_sync_flag = true;
  }
}

class ClDataSet_ACandles_HighCharts extends ClDataSet_ACandles
{
  constructor(recs_size, gui_chart, wreq_args)
  {
    super(recs_size, wreq_args);
    this.loc_gui_chart = gui_chart;
    this.loc_gui_recn  = 0;
    this.loc_mts_sync  = -1;
    this.loc_sync_flag = false;
  }

  onSyncDataGUI_impl()
  {
//    this.loc_gui_chart.redraw({});
  }

  onLocDataAppend_CB(chan_data)
  {
    if (!this.loc_sync_flag) {
      return 0;
    }
    this.onSyncDataGUI_impl();
    this.loc_sync_flag = false;
  }

  onLocRecAdd_CB(flag_sece, candle_rec, rec_index)
  {
    var gui_sers, pnt_this;
    gui_sers = this.loc_gui_chart.series[0];
    pnt_this = [
        candle_rec.mts,
        candle_rec.open,
        candle_rec.high,
        candle_rec.low,
        candle_rec.close,
      ];
    /*
    if (candle_rec.mts >  this.loc_mts_sync) {
      gui_sers.addPoint(pnt_this, flag_sece);
      this.loc_mts_sync  = candle_rec.mts;
    }
    else {
      var gui_index = gui_sers.data.length + rec_index - this.loc_candle_recs.length;
      if ((gui_index >= 0) && (gui_index <  gui_sers.data.length)) {
        gui_sers.data[gui_index].update(pnt_this, flag_sece);
      }
    }
    // */
    gui_sers.addPoint(pnt_this, flag_sece);
    this.loc_mts_sync  = candle_rec.mts;
    this.loc_sync_flag = true;
  }
}

