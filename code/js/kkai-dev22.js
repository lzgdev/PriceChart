
// HighCharts sample code: javascript

class ClChanData_ABooks_HighCharts extends ClChanData_ABooks
{
  constructor(wreq_prec, wreq_len, gui_chart)
  {
    super(wreq_prec, wreq_len)
    this.loc_gui_chart = gui_chart;
    this.loc_sync_flag = false;
    this.loc_num_bids = 0;
    this.loc_num_pads = 0;
    this.loc_num_asks = 0;
this.num_change = 0;
  }

  onSyncDataGUI_impl()
  {
    var  idx_book, idx_sers;
    for (idx_sers=0; idx_sers < 3; idx_sers++)
    {
      var  gui_sers, loc_book, flag_bids;
      var  num_pnt_gui, num_pnt_loc;
      var  pric_this, val_next;
      var  pnt_this, num_next, idx_next;
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
      num_pnt_gui = gui_sers.data.length;
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
        pnt_this = { x: pric_this, y: loc_book[idx_book].sumamt, };
        if (num_pnt_loc+1 <  num_pnt_gui) {
          gui_sers.data[num_pnt_loc].update(pnt_this, false);
        }
        else {
          gui_sers.addPoint(pnt_this, false);
          num_pnt_gui ++;
        }
        num_pnt_loc ++;
        for (idx_next=1; idx_next <  num_next; idx_next++)
        {
          pnt_this = { x: Number(pric_this + this.loc_book_unit * idx_next).toFixed(1), y: val_next, };
          if (num_pnt_loc+1 < num_pnt_gui) {
            gui_sers.data[num_pnt_loc].update(pnt_this, false);
          }
          else {
            gui_sers.addPoint(pnt_this, false);
            num_pnt_gui ++;
          }
          num_pnt_loc ++;
        }
      }
      while (num_pnt_gui >  num_pnt_loc)
      {
        gui_sers.removePoint(num_pnt_gui-1, false);
        num_pnt_gui --;
      }
    }
    this.loc_gui_chart.redraw({});
  }

  onLocCleanData_CB()
  {
    this.loc_num_bids = 0;
    this.loc_num_pads = 0;
    this.loc_num_asks = 0;
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

  onLocBookChg_CB(book_rec, flag_bids, idx_book, flag_del)
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

