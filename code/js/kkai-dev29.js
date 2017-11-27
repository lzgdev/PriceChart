
import { ClChanData_ACandles_AnyChart, ClChanData_ABooks_AnyChart, } from './kkai-dev21.js';
import { ClChanData_ACandles_HighCharts, ClChanData_ABooks_HighCharts, } from './kkai-dev22.js';
import { ClChanData_ACandles_GoogleCharts, ClChanData_ABooks_GoogleCharts, } from './kkai-dev23.js';
import { ClNetClient_BfxWss, ClNetClient_Base, } from './kkai-dev31.js';

var obj_netclient = null;

var mapWREQs = [
/*
        { channel:    'book', uid: 'container-bookP0', prec: 'P0', len: 100, visible: true, },
        { channel:    'book', uid: 'container-bookP1', prec: 'P1', len: 100, visible: true, },
        { channel: 'candles', uid: 'container-candle', key: 'trade:1m:tBTCUSD', visible: true, },
// */
        { channel:    'book', uid: 'container-bookP0', prec: 'P0', len: 100, visible:  true, },
        { channel:    'book', uid: 'container-bookP1', prec: 'P1', len: 100, visible: false, },
        { channel: 'candles', uid: 'container-candle', key: 'trade:1m:tBTCUSD', visible: true, },
      ];

function cbEV_OnDocReady_prep()
{
  obj_netclient = new ClNetClient_BfxWss();
}

function cbEV_OnDocReady_anychart()
{
  var mi;

  for (mi=0; mi < mapWREQs.length; mi++)
  {
    var chan_obj, chart_gui;
    var map_unit = mapWREQs[mi];
    if (!map_unit.visible) {
      continue;
    }
    chan_obj = null;
    if (map_unit.channel == 'book') {
      var series;
      chart_gui = anychart.area();
      chart_gui.yScale().stackMode("value");
      chan_obj = new ClChanData_ABooks_AnyChart(chart_gui, map_unit.prec, map_unit.len);

      // create a step area series and set the data
      series = chart_gui.stepArea(chan_obj.loc_map_bids);
      series.stepDirection("backward");
      series = chart_gui.stepArea(chan_obj.loc_map_asks);
      series.stepDirection("forward");

      chart_gui.container(map_unit.uid);
      chart_gui.draw();
    }
    else
    if (map_unit.channel == 'candles') {
//    https://docs.anychart.com/Stock_Charts/Series/Japanese_Candlestick
      var series;
      chart_gui = anychart.stock();

      chan_obj = new ClChanData_ACandles_AnyChart(1000, chart_gui, map_unit.key);

      // set the series
      series = chart_gui.plot(0).candlestick(chan_obj.loc_map_ohlc);
      series.name("Prices");
      series = chart_gui.plot(1).column(chan_obj.loc_map_vol);
      series.name("Volumns");

      chart_gui.container(map_unit.uid);
      chart_gui.draw();
    }

    if ((chan_obj != null) && (obj_netclient != null)) {
      obj_netclient.addObj_DataReceiver(chan_obj);
    }
  }
}

function cbEV_OnDocReady_highcharts()
{
  var mi;

  for (mi=0; mi < mapWREQs.length; mi++)
  {
    var chan_obj, chart_gui;
    var map_unit = mapWREQs[mi];
    if (!map_unit.visible) {
      continue;
    }
    chan_obj = null;
    if (map_unit.channel == 'book') {
      chart_gui = Highcharts.chart(map_unit.uid, {
        chart: {
            type: 'area',
            backgroundColor: '#1F1F1F',
        },
        title: {
            text: 'Books Depth: ' + map_unit.prec,
        },
        yAxis: {
            min: 0.0,
            title: {
                text: 'Amount Sum',
            },
            stackLabels: {
                enabled: true,
                style: {
                    fontWeight: 'bold',
                    color: (Highcharts.theme && Highcharts.theme.textColor) || 'gray'
                }
            }
        },
        tooltip: {
            headerFormat: '<b>{point.x}</b><br/>',
            pointFormat: '{series.name}: {point.y}<br/>Total: {point.stackTotal}'
        },
        plotOptions: {
            column: {
                stacking: 'normal',
                dataLabels: {
                    enabled: true,
                    color: (Highcharts.theme && Highcharts.theme.dataLabelsColor) || 'white'
                }
            }
        },
        series: [
          {
            name: 'Bids',
            step: true,
            color: '#009F00',
            data: [ ],
          },
          {
            name: 'Tick',
            step: true,
            color: '#FFD700',
            data: [ ],
          },
          {
            name: 'Asks',
            step: true,
            color: '#9F0000',
            data: [ ],
          },
        ],
      });
      chan_obj = new ClChanData_ABooks_HighCharts(chart_gui, map_unit.prec, map_unit.len);
    }
    else
    if (map_unit.channel == 'candles') {
      chart_gui = Highcharts.stockChart(map_unit.uid, {
        chart: {
            backgroundColor: '#1F1F1F',
          },
        plotOptions: {
            candlestick: {
              color:   '#9F0000',	    		
              upColor: '#009F00',
	        },
          },
        title: {
            text: 'History: ' + map_unit.key,
          },
        subtitle: {
            text: 'Subtitle ...',
          },
        tooltip: {
            headerFormat: '<b>{point.x}</b><br/>',
            pointFormat: '{series.name}: {point.y}<br/>Total: {point.stackTotal}'
          },
        series: [
            {
              type: 'candlestick',
              data: [ ],
              dataGrouping: {
                enabled: false
              }
            },
          ],
        });
      chan_obj = new ClChanData_ACandles_HighCharts(1000, chart_gui, map_unit.key);
    }

    if ((chan_obj != null) && (obj_netclient != null)) {
      obj_netclient.addObj_DataReceiver(chan_obj);
    }
  }
}

function cbEV_OnDocReady_googlecharts()
{
  var mi;

  for (mi=0; mi < mapWREQs.length; mi++)
  {
    var chan_obj, chart_gui;
    var map_unit = mapWREQs[mi];
    if (!map_unit.visible) {
      continue;
    }
    chan_obj = null;
    if (map_unit.channel == 'book') {
      chart_gui = Highcharts.chart(map_unit.uid, {
        chart: {
            type: 'area',
            backgroundColor: '#1F1F1F',
        },
        title: {
            text: 'Books Depth: ' + map_unit.prec,
        },
        yAxis: {
            min: 0.0,
            title: {
                text: 'Amount Sum',
            },
            stackLabels: {
                enabled: true,
                style: {
                    fontWeight: 'bold',
                    color: (Highcharts.theme && Highcharts.theme.textColor) || 'gray'
                }
            }
        },
        tooltip: {
            headerFormat: '<b>{point.x}</b><br/>',
            pointFormat: '{series.name}: {point.y}<br/>Total: {point.stackTotal}'
        },
        plotOptions: {
            column: {
                stacking: 'normal',
                dataLabels: {
                    enabled: true,
                    color: (Highcharts.theme && Highcharts.theme.dataLabelsColor) || 'white'
                }
            }
        },
        series: [
          {
            name: 'Bids',
            step: true,
            color: '#009F00',
            data: [ ],
          },
          {
            name: 'Tick',
            step: true,
            color: '#FFD700',
            data: [ ],
          },
          {
            name: 'Asks',
            step: true,
            color: '#9F0000',
            data: [ ],
          },
        ],
      });
      chan_obj = new ClChanData_ABooks_GoogleCharts(chart_gui, map_unit.prec, map_unit.len);
    }
    else
    if (map_unit.channel == 'candles') {
      var options = {
        legend:'none'
      };
      chart_gui = new google.visualization.CandlestickChart(document.getElementById(map_unit.uid));
      chan_obj  = new ClChanData_ACandles_GoogleCharts(1000, chart_gui, map_unit.key);
      chart_gui.draw(chan_obj.loc_gui_datatable, options);
    }

    if ((chan_obj != null) && (obj_netclient != null)) {
      obj_netclient.addObj_DataReceiver(chan_obj);
    }
  }
}

function cbEV_OnDocReady_websocket()
{
  obj_netclient.netClient_exec();
}

function _onUI_Test01()
{
  $('#log_out1').html('Close websocket.');
  num_msg_max = -1;
}

function _onUI_Test02()
{
  $('#log_out1').html('Test 02.');
}

export { cbEV_OnDocReady_prep, };

export { cbEV_OnDocReady_anychart, };
export { cbEV_OnDocReady_highcharts, };
export { cbEV_OnDocReady_googlecharts, };

export { cbEV_OnDocReady_websocket, };

export { _onUI_Test01, _onUI_Test02, };

