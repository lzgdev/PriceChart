
var obj_netclient = null;

var mapWREQs = [
        { channel:  'ticker', uid: 'container-ticker', visible: false, wreq_args: { symbol: 'tBTCUSD', }, },
        { channel:    'book', uid: 'container-bookP0', visible: false, wreq_args: { symbol: 'tBTCUSD', prec: 'P0', freq: 'F1', len: '100', }, },
        { channel:    'book', uid: 'container-bookP1', visible: false, wreq_args: { symbol: 'tBTCUSD', prec: 'P1', freq: 'F1', len: '100', }, },
        { channel:    'book', uid: 'container-bookP2', visible: false, wreq_args: { symbol: 'tBTCUSD', prec: 'P2', freq: 'F1', len: '100', }, },
        { channel:    'book', uid: 'container-bookP3', visible: false, wreq_args: { symbol: 'tBTCUSD', prec: 'P3', freq: 'F1', len: '100', }, },
        { channel: 'candles', uid: 'container-candle', visible: false, wreq_args: {    key: 'trade:1m:tBTCUSD', }, },
      ];

function cbEV_OnDocReady_prep(url_wss=null)
{
  obj_netclient = new ClNetClient_BfxWss(url_wss);
}

function cbEV_OnDocReady_highcharts()
{
  var mi;

  for (mi=0; mi < mapWREQs.length; mi++)
  {
    var map_unit = mapWREQs[mi], gui_unit;
    gui_unit = document.getElementById(map_unit.uid);
    map_unit.visible = (gui_unit == null || gui_unit.hidden) ? false : true;
  }

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
            step: 'right',
            color: '#009F00',
            data: [ ],
          },
          {
            name: 'Tick',
            step: 'center',
            color: '#FFD700',
            data: [ ],
          },
          {
            name: 'Asks',
            step: 'left',
            color: '#9F0000',
            data: [ ],
          },
        ],
      });
      chan_obj = new ClDataSet_ABooks_HighCharts(chart_gui, map_unit.wreq_args);
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
              name: 'OHLC',
              data: [ ],
              dataGrouping: {
                enabled: false
              }
            },
/*
            {
              type: 'column',
              name: 'VOL',
              data: [ ],
              dataGrouping: {
                enabled: false
              }
            },
// */
          ],
        });
      chan_obj = new ClDataSet_ACandles_HighCharts(1000, chart_gui, map_unit.wreq_args);
    }

    if ((chan_obj != null) && (obj_netclient != null)) {
      obj_netclient.addObj_DataReceiver(chan_obj);
    }
  }
}

function cbEV_OnDocReady_websocket()
{
  if (obj_netclient.ncChk_HaveRecvs()) {
    obj_netclient.ncOP_Exec();
  }
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

