function cbEV_OnDocReady_highcharts()
{
  var map_wreq2uid = [
        { prec: 'P0', len:  25, uid: 'dep-book-P0', visible:  true, },
        { prec: 'P1', len:  25, uid: 'dep-book-P1', visible: false, },
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
        type: 'column'
    },
    title: {
        text: 'Stacked column chart'
    },
    xAxis: {
//        categories: ['Apples', 'Oranges', 'Pears', 'Grapes', 'Bananas']
    },
    yAxis: {
        min: 0,
        title: {
            text: 'Total fruit consumption'
        },
        stackLabels: {
            enabled: true,
            style: {
                fontWeight: 'bold',
                color: (Highcharts.theme && Highcharts.theme.textColor) || 'gray'
            }
        }
    },
    legend: {
        align: 'right',
        x: -30,
        verticalAlign: 'top',
        y: 25,
        floating: true,
        backgroundColor: (Highcharts.theme && Highcharts.theme.background2) || 'white',
        borderColor: '#CCC',
        borderWidth: 1,
        shadow: false
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
        name: 'John',
        pointPadding: 0,
        groupPadding: 0,
//pointPlacement: 'between',
//        data: [ 5, 3, 4, 7, 2, ],
        data: [ ],
      },
      {
        name: 'Jane',
        pointPadding: 0,
        groupPadding: 0,
//pointPlacement: 'between',
//        data: [ 2, 5, 1, 2, 4, ],
        data: [ ],
      },
    ],
  });

var data1 = [ 5, 3, 4, 7, 2, ];
var data2 = [ 2, 5, 1, 2, 4, ];
highcharts_chart.series[0].setData(data1, false);
highcharts_chart.series[1].setData(data2, false);
highcharts_chart.redraw({});
}

/*
    {
        name: 'Histogram',
        type: 'column',
        data: histogram(data, 10),
        pointPadding: 0,
        groupPadding: 0,
        pointPlacement: 'between'
    }
// */

