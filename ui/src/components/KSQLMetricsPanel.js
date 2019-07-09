import React from 'react'
import _ from 'lodash'
import axios from 'axios'
import { DataChart } from './DataChart'

import 'hammerjs'

export default class KSQLMetricsPanel extends React.Component {
  constructor(props) {
    super(props)
    this.state = {
      'consumer-total-messages': [],
      'consumer-messages-per-sec': [],
      'total-messages': [],
      'messages-per-sec': [],
      'consumer-failed-messages': [],
      'failed-messages-per-sec': [],
    }
    this.interval = null
  }

  customSetInterval = (limit, interval) => {
    return setInterval(() => {
      axios.get(`http://${process.env.REACT_APP_SERVER_HOST}:5000/ksql_metrics?resource=${this.props.resource}`).then(res => {
        const data = {}
        Object.keys(res.data).map(metric_name => {
          data[metric_name] = [...(this.state[metric_name]), res.data[metric_name]]
          if (data[metric_name].length === limit + 1)
            data[metric_name] = _.drop(data[metric_name], 1)
          return null
        })
        this.setState({...data})
      })
    }, interval)
  }

  strToHumanReadable = (str, delimiter) => _.join(_.split(str, delimiter).map(s => _.capitalize(s)), ' ')

  componentDidMount() {
    this.interval = this.customSetInterval(30, 2000)
  }

  componentWillUnmount() {
    clearInterval(this.interval)
  }

  render() {
    const metric_list_dom = Object.keys(this.state).map((metric_name, i) => {
      const title = this.strToHumanReadable(metric_name, '-')
      return <DataChart key={i} title={title} data={this.state[metric_name]} height="200px"></DataChart>
    })
    return (
      <div>
        <h2>{this.strToHumanReadable(this.props.resource, '_')}</h2>
        {metric_list_dom}
      </div>
    )
  }
}
