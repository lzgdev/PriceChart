function cbEV_OnDocReady_highcharts()
{
  var map_wreq2uid = [
//        { prec: 'P0', len:  25, uid: 'dep-book-P0', visible:  true, },
        { prec: 'P0', len: 100, uid: 'dep-book-P0', visible:  true, },
//        { prec: 'P1', len:  25, uid: 'dep-book-P1', visible: false, },
//        { prec: 'P1', len:  25, uid: 'dep-book-P1', visible: true, },
      ];

  for (var m=0; m < map_wreq2uid.length; m++)
  {
    var books_obj;
    var chart;
    var series, seriesData;
    var map_unit = map_wreq2uid[m];
    if (!map_unit.visible) {
      continue;
    }
    books_obj = new ClChanData_ABooks_HighCharts(map_unit.prec, map_unit.len);
    chan_book_OBJs.push(books_obj);
  }

  highcharts_chart = Highcharts.chart('container', {
    chart: {
        type: 'area',
        backgroundColor: '#1F1F1F',
    },
    title: {
        text: 'Books Depth View',
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
        zoneAxis: 'x',
        color: '#009F00',
        data: [ ],
      },
      {
        name: 'Asks',
        zoneAxis: 'x',
        color: '#9F0000',
        data: [ ],
      },
    ],
  });
}

