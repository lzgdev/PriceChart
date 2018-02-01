// module kkai-dev22

var __gmark_runintv_guisync_abooks = false;
var __gmark_runintv_guisync_stamp  = 1;
var __glist_runintv_guisync_abooks = [];

function __runintv_guisync_abooks()
{
  var idx_abooks;
  __gmark_runintv_guisync_stamp ++;
  //console.log("gui sync: " + __gmark_runintv_guisync_stamp + " ...");
  for (idx_abooks=__glist_runintv_guisync_abooks.length-1; idx_abooks >= 0; idx_abooks--)
  {
    var flag_del = false, flag_syn = false;
    var obj_abooks = __glist_runintv_guisync_abooks[idx_abooks];
    if (obj_abooks.loc_sync_mark == 0) {
      flag_del =  true;
    }
    else
    if (__gmark_runintv_guisync_stamp >= (obj_abooks.loc_sync_mark+2)) {
      flag_del =  true;
      flag_syn =  true;
    }
    if (flag_syn) {
      obj_abooks.syncDataGUI();
    }
    if (flag_del) {
      __glist_runintv_guisync_abooks.splice(idx_abooks, 1);
    }
  }
}

class ClDataSet_Stat_HighCharts extends ClDataSet_Stat
{
  constructor(recs_size, gui_chart, wreq_args)
  {
    super(recs_size, wreq_args);
    this.loc_gui_chart = gui_chart;
    this.loc_gui_recn  = 0;
    this.loc_mts_sync  = -1;
  }

  onLocDataClean_CB()
  {
    this.loc_gui_recn  = 0;
  }

  onLocRecAdd_CB(flag_plus, candle_rec, rec_index)
  {
    var idx_gui;
    var gui_ohlc, pnt_ohlc, gui_vol, pnt_vol;
    if (!flag_plus) {
      return 0;
    }
    gui_ohlc = (this.loc_gui_chart.series.length <= 0) ? null : this.loc_gui_chart.series[0];
    gui_vol  = (this.loc_gui_chart.series.length <= 1) ? null : this.loc_gui_chart.series[1];
    pnt_ohlc = [
        candle_rec.mts,
        candle_rec.open,
        candle_rec.high,
        candle_rec.low,
        candle_rec.close,
      ];
    pnt_vol = [
        candle_rec.mts,
        candle_rec.volume,
      ];
    if (candle_rec.mts >  this.loc_mts_sync) {
      gui_ohlc.addPoint(pnt_ohlc, true);
      this.loc_mts_sync  = candle_rec.mts;
      this.loc_gui_recn ++;
    }
    else
    if ((idx_gui=gui_ohlc.data.length-1) >= 0) {
      if (gui_ohlc.data[idx_gui].x == candle_rec.mts) {
        gui_ohlc.data[idx_gui].update(pnt_ohlc, true);
      }
    }
    return 1;
  }

  onLocDataSync_CB()
  {
    var idx_rec, num_rec;
    var candle_rec, gui_ohlc, pnt_ohlc, gui_vol, pnt_vol;
    gui_ohlc = (this.loc_gui_chart.series.length <= 0) ? null : this.loc_gui_chart.series[0];
    gui_vol  = (this.loc_gui_chart.series.length <= 1) ? null : this.loc_gui_chart.series[1];
    num_rec  = this.loc_candle_recs.length;
    for (idx_rec=0; idx_rec <  num_rec; idx_rec++)
    {
      candle_rec = this.loc_candle_recs[idx_rec];
      if (gui_ohlc != null) {
        pnt_ohlc = [
            candle_rec.mts,
            candle_rec.open,
            candle_rec.high,
            candle_rec.low,
            candle_rec.close,
          ];
        gui_ohlc.addPoint(pnt_ohlc, false);
      }
      if (gui_vol != null) {
        pnt_vol = [
            candle_rec.mts,
            candle_rec.volume,
          ];
        gui_vol.addPoint(pnt_vol, false);
      }
      this.loc_mts_sync  = candle_rec.mts;
      this.loc_gui_recn ++;
    }
    this.loc_gui_chart.redraw({});
  }
}


class ClDataSet_Ticker_HighCharts extends ClDataSet_Ticker
{
  constructor(gui_chart, wreq_args)
  {
    super(wreq_args);
    this.loc_gui_chart = gui_chart;
  }

  onLocRecAdd_CB(ticker_rec, rec_index)
  {
  }
}

class ClDataSet_ATrades_HighCharts extends ClDataSet_ATrades
{
  constructor(recs_size, gui_chart, wreq_args)
  {
    super(recs_size, wreq_args);
    this.loc_gui_chart = gui_chart;
    this.loc_gui_recn  = 0;
    this.loc_mts_sync  = -1;
  }

  onLocDataClean_CB()
  {
    this.loc_gui_recn  = 0;
  }

  onLocRecAdd_CB(flag_plus, candle_rec, rec_index)
  {
    var idx_gui;
    var gui_ohlc, pnt_ohlc, gui_vol, pnt_vol;
    if (!flag_plus) {
      return 0;
    }
    gui_ohlc = (this.loc_gui_chart.series.length <= 0) ? null : this.loc_gui_chart.series[0];
    gui_vol  = (this.loc_gui_chart.series.length <= 1) ? null : this.loc_gui_chart.series[1];
    pnt_ohlc = [
        candle_rec.mts,
        candle_rec.open,
        candle_rec.high,
        candle_rec.low,
        candle_rec.close,
      ];
    pnt_vol = [
        candle_rec.mts,
        candle_rec.volume,
      ];
    if (candle_rec.mts >  this.loc_mts_sync) {
      gui_ohlc.addPoint(pnt_ohlc, true);
      this.loc_mts_sync  = candle_rec.mts;
      this.loc_gui_recn ++;
    }
    else
    if ((idx_gui=gui_ohlc.data.length-1) >= 0) {
      if (gui_ohlc.data[idx_gui].x == candle_rec.mts) {
        gui_ohlc.data[idx_gui].update(pnt_ohlc, true);
      }
    }
    return 1;
  }

  onLocDataSync_CB()
  {
    var idx_rec, num_rec;
    var candle_rec, gui_ohlc, pnt_ohlc, gui_vol, pnt_vol;
    gui_ohlc = (this.loc_gui_chart.series.length <= 0) ? null : this.loc_gui_chart.series[0];
    gui_vol  = (this.loc_gui_chart.series.length <= 1) ? null : this.loc_gui_chart.series[1];
    num_rec  = this.loc_candle_recs.length;
    for (idx_rec=0; idx_rec <  num_rec; idx_rec++)
    {
      candle_rec = this.loc_candle_recs[idx_rec];
      if (gui_ohlc != null) {
        pnt_ohlc = [
            candle_rec.mts,
            candle_rec.open,
            candle_rec.high,
            candle_rec.low,
            candle_rec.close,
          ];
        gui_ohlc.addPoint(pnt_ohlc, false);
      }
      if (gui_vol != null) {
        pnt_vol = [
            candle_rec.mts,
            candle_rec.volume,
          ];
        gui_vol.addPoint(pnt_vol, false);
      }
      this.loc_mts_sync  = candle_rec.mts;
      this.loc_gui_recn ++;
    }
    this.loc_gui_chart.redraw({});
  }
}

class ClDataSet_ABooks_HighCharts extends ClDataSet_ABooks
{
  constructor(gui_chart, wreq_args)
  {
    super(wreq_args);
    this.loc_gui_chart = gui_chart;
    this.loc_sync_mark = 0;
    this.loc_min_xaxis =  0.0;
    this.loc_max_xaxis = -1.0;
    this.loc_min_yaxis =  0.0;
    this.loc_max_yaxis = -1.0;

    this.num_change = 0;
  }

  syncDataGUI()
  {
    this.onSyncDataGUI_impl();
    this.loc_sync_mark = 0;
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

  onLocRecAdd_CB(flag_plus, book_rec, flag_bids, idx_book, flag_del)
  {
    if (!flag_plus) {
      return 0;
    }
    if (!__gmark_runintv_guisync_abooks) {
      __gmark_runintv_guisync_abooks = true;
      setInterval(__runintv_guisync_abooks, 25);
    }
    this.loc_sync_mark = __gmark_runintv_guisync_stamp;
    if (__glist_runintv_guisync_abooks.indexOf(this) < 0) {
      __glist_runintv_guisync_abooks.push(this);
    }
    return 1;
  }

  onLocDataSync_CB()
  {
    this.syncDataGUI();
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
  }

  onLocDataClean_CB()
  {
    this.loc_gui_recn  = 0;
  }

  onLocRecAdd_CB(flag_plus, candle_rec, rec_index)
  {
    var idx_gui;
    var gui_ohlc, pnt_ohlc, gui_vol, pnt_vol;
    if (!flag_plus) {
      return 0;
    }
    gui_ohlc = (this.loc_gui_chart.series.length <= 0) ? null : this.loc_gui_chart.series[0];
    gui_vol  = (this.loc_gui_chart.series.length <= 1) ? null : this.loc_gui_chart.series[1];
    pnt_ohlc = [
        candle_rec.mts,
        candle_rec.open,
        candle_rec.high,
        candle_rec.low,
        candle_rec.close,
      ];
    pnt_vol = [
        candle_rec.mts,
        candle_rec.volume,
      ];
    if (candle_rec.mts >  this.loc_mts_sync) {
      gui_ohlc.addPoint(pnt_ohlc, true);
      this.loc_mts_sync  = candle_rec.mts;
      this.loc_gui_recn ++;
    }
    else
    if ((idx_gui=gui_ohlc.data.length-1) >= 0) {
      if (gui_ohlc.data[idx_gui].x == candle_rec.mts) {
        gui_ohlc.data[idx_gui].update(pnt_ohlc, true);
      }
    }
    return 1;
  }

  onLocDataSync_CB()
  {
    var idx_rec, num_rec;
    var candle_rec, gui_ohlc, pnt_ohlc, gui_vol, pnt_vol;
    gui_ohlc = (this.loc_gui_chart.series.length <= 0) ? null : this.loc_gui_chart.series[0];
    gui_vol  = (this.loc_gui_chart.series.length <= 1) ? null : this.loc_gui_chart.series[1];
    num_rec  = this.loc_candle_recs.length;
    for (idx_rec=0; idx_rec <  num_rec; idx_rec++)
    {
      candle_rec = this.loc_candle_recs[idx_rec];
      if (gui_ohlc != null) {
        pnt_ohlc = [
            candle_rec.mts,
            candle_rec.open,
            candle_rec.high,
            candle_rec.low,
            candle_rec.close,
          ];
        gui_ohlc.addPoint(pnt_ohlc, false);
      }
      if (gui_vol != null) {
        pnt_vol = [
            candle_rec.mts,
            candle_rec.volume,
          ];
        gui_vol.addPoint(pnt_vol, false);
      }
      this.loc_mts_sync  = candle_rec.mts;
      this.loc_gui_recn ++;
    }
    this.loc_gui_chart.redraw({});
  }
}

