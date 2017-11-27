// module kkai-dev21

import { ClDataSet_ACandles, ClDataSet_ABooks, } from './kkai-dev11.js';

class ClDataSet_ABooks_AnyChart extends ClDataSet_ABooks
{
  constructor(gui_chart, wreq_prec, wreq_len)
  {
    super(wreq_prec, wreq_len)
    this.loc_gui_chart = gui_chart;

    this.loc_gui_dataset_bids = anychart.data.set();
    this.loc_gui_dataset_asks = anychart.data.set();
    this.loc_map_bids = this.loc_gui_dataset_bids.mapAs(
        { x: 0, value: 1, });
    this.loc_map_asks = this.loc_gui_dataset_asks.mapAs(
        { x: 0, value: 1, });

    this.loc_sync_flag = false;
    this.loc_min_xaxis =  0.0;
    this.loc_max_xaxis = -1.0;
    this.loc_min_yaxis =  0.0;
    this.loc_max_yaxis = -1.0;
this.num_change = 0;
  }

  onSyncDataGUI_impl()
  {
this.num_change ++;
if (this.num_change > 50) { return; }
    var  idx_book, idx_sers;
    for (idx_sers=0; idx_sers < 3; idx_sers++)
    {
      var  gui_dataset, loc_book, flag_bids;
      var  num_pnt_gui, num_pnt_loc;
      var  pric_this, val_next;
      var  pnt_this, num_next, idx_next;
      if (idx_sers == 0) {
        flag_bids = true;
        gui_dataset = this.loc_gui_dataset_bids;
        loc_book  = this.loc_book_bids;
      }
      else
      if (idx_sers == 2) {
        flag_bids = false;
        gui_dataset = this.loc_gui_dataset_asks;
        loc_book  = this.loc_book_asks;
      }
      else {
        continue;
      }
      num_pnt_gui = gui_dataset.getRowsCount();
      num_pnt_loc = 0;
      for (idx_book=0; idx_book <  loc_book.length; idx_book++)
      {
        pric_this =  loc_book[idx_book].price;
        num_next  = (idx_book+1 >= loc_book.length) ? 0 : Math.round(
                        (loc_book[idx_book+1].price - pric_this) / this.loc_book_unit);
        // evaluate val_next
        if ( flag_bids && idx_book+1 <  loc_book.length) {
          val_next = loc_book[idx_book+1].sumamt;
        }
        else
        if (!flag_bids) {
          val_next = loc_book[idx_book].sumamt;
        }
        else {
          val_next = null;
        }
        pnt_this = [ pric_this, loc_book[idx_book].sumamt, ];
        if (num_pnt_loc+1 <  num_pnt_gui) {
          gui_dataset.row(num_pnt_loc, pnt_this);
        }
        else {
          gui_dataset.append(pnt_this);
          num_pnt_gui ++;
        }
        num_pnt_loc ++;
        for (idx_next=1; idx_next <  num_next; idx_next++)
        {
          pnt_this = [ Number(pric_this + this.loc_book_unit * idx_next).toFixed(1), val_next, ];
          if (num_pnt_loc+1 < num_pnt_gui) {
            gui_dataset.row(num_pnt_loc, pnt_this);
          }
          else {
            gui_dataset.append(pnt_this);
            num_pnt_gui ++;
          }
          num_pnt_loc ++;
        }
      }
      while (num_pnt_gui >  num_pnt_loc)
      {
        gui_dataset.remove(num_pnt_gui-1);
        num_pnt_gui --;
      }
    }

/*
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
// */
  }

  onLocCleanData_CB()
  {
  }

  onLocAppendData_CB(chan_data)
  {
    if (!this.loc_sync_flag) {
      return 0;
    }
this.num_change ++;
if ((this.num_change % 4) != 0) { return -1; }
    this.onSyncDataGUI_impl();
    this.loc_sync_flag = false;
  }

  onLocRecChg_CB(flag_sece, book_rec, flag_bids, idx_book, flag_del)
  {
    this.loc_sync_flag = true;
  }

  onSyncDataGUI_impl2()
  {
    var  idx_book, idx_sers;
    for (idx_sers=0; idx_sers <  3; idx_sers++)
    {
      var  gui_sers, loc_book, flag_bids;
      var  gui_data;
      var  pric_this, val_next;
      var  num_next, idx_next;
      if (idx_sers == 0) {
        flag_bids = true;
        loc_book  = this.loc_book_bids;
      }
      else
      if (idx_sers == 2) {
        flag_bids = false;
        loc_book  = this.loc_book_asks;
      }
      else {
        continue;
      }
      gui_sers = this.loc_gui_chart.series[idx_sers];
      gui_data = [];
      for (idx_book=0; idx_book <  loc_book.length; idx_book++)
      {
        pric_this =  loc_book[idx_book].price;
        num_next  = (idx_book+1 >= loc_book.length) ? 0 : Math.round(
                        (loc_book[idx_book+1].price - pric_this) / this.loc_book_unit);
        // evaluate val_next
        if ( flag_bids && idx_book+1 <  loc_book.length) {
          val_next = loc_book[idx_book+1].sumamt;
        }
        else
        if (!flag_bids) {
          val_next = loc_book[idx_book].sumamt;
        }
        else {
          val_next = null;
        }
        gui_data.push({ x: pric_this, y: loc_book[idx_book].sumamt, });
        for (idx_next=1; idx_next <  num_next; idx_next++) {
          gui_data.push({ x: Number(pric_this + this.loc_book_unit * idx_next).toFixed(1), y: val_next, });
        }
      }
      gui_sers.setData(gui_data, false);
    }
    this.loc_gui_chart.redraw({});
  }

    // TEMP: temp develop/debug code
    /*
    var err_ses = null;
    var arr_errors = this.devCheck_Books(err_ses);
    for (var idx_err = 0; idx_err < arr_errors.length; idx_err++) {
      $('#log_out2').append('\n' + arr_errors[idx_err]);
    }
    // */
}

class ClDataSet_ACandles_AnyChart extends ClDataSet_ACandles
{
  constructor(recs_size, gui_chart, wreq_key)
  {
    super(recs_size, wreq_key);
    this.loc_gui_chart = gui_chart;
    this.loc_gui_datatable = anychart.data.table();
    this.loc_map_ohlc = this.loc_gui_datatable.mapAs(
        { 'x': 0, 'open': 1, 'high': 2, 'low': 3, 'close': 4, });
    this.loc_map_vol  = this.loc_gui_datatable.mapAs(
        { 'x': 0, 'value': 5, });
    this.loc_mts_sync  = -1;
    this.loc_sync_flag = false;
  }

  onSyncDataGUI_impl()
  {
  }

  onLocAppendData_CB(chan_data)
  {
    if (!this.loc_sync_flag) {
      return 0;
    }
    this.onSyncDataGUI_impl();
    this.loc_sync_flag = false;
  }

  onLocRecChg_CB(flag_sece, candle_rec, rec_index)
  {
    this.loc_gui_datatable.addData([[ candle_rec.mts,
        candle_rec.open,
        candle_rec.high,
        candle_rec.low,
        candle_rec.close,
        candle_rec.volume,
      ]]);
    this.loc_mts_sync  = candle_rec.mts;
    this.loc_sync_flag = true;
  }
}

export { ClDataSet_ACandles_AnyChart, ClDataSet_ABooks_AnyChart, };

