import React from 'react'

import 'hammerjs';

import { Chart, ChartSeries, ChartSeriesItem, ChartTitle } from '@progress/kendo-react-charts';

export const DataChart = props => (
  <Chart style={{ height: props.height || '300px' }} transitions={false}>
    <ChartTitle text={props.title} />
    <ChartSeries>
      <ChartSeriesItem
        key="1"
        type="line"
        stype="smooth"
        data={props.data}
      />
    </ChartSeries>
  </Chart>
);
