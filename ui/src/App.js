import React, { Component } from 'react'
import ReactDOM from 'react-dom';
import axios from 'axios'
import { Input } from '@progress/kendo-react-inputs' 
import { Button } from '@progress/kendo-react-buttons'
import { Ripple } from '@progress/kendo-react-ripple'
import KSQLMetricsPanel from './components/KSQLMetricsPanel'
import { savePDF } from '@progress/kendo-react-pdf';

import '@progress/kendo-theme-material/dist/all.css'
import './App.css'
import 'bootstrap-4-grid/css/grid.min.css'

class App extends Component {
  constructor(props) {
    super(props)
    this.appContainer = React.createRef()
    this.state = {
      brokers_info: { amount: 0, instance_type: '' },
      ksqls_info: { amount: 0, instance_type: '' },
      num_parallel_producer_processes: 0,
      num_partitions: 0,
    }
  }

  handlePDFExport = () => {
    savePDF(ReactDOM.findDOMNode(this.appContainer), { paperSize: 'auto' });
  }

  handleRun = (event) => {
    event.preventDefault()
    axios.post(`http://${process.env.REACT_APP_SERVER_HOST}:5000/run`)
  }

  componentDidMount() {
    this.interval = setInterval(() => {
      axios.get(`http://${process.env.REACT_APP_SERVER_HOST}:5000/configs`).then(res => {
        this.setState({
          brokers_info: {...res.data.clusters.brokers},
          ksqls_info: {...res.data.clusters.ksqls},
          num_parallel_producer_processes: res.data.num_parallel_producer_processes,
          num_partitions: res.data.num_partitions,
        })
        this.setState({...(res.data)})
      })
    }, 5000)
  }

  componentWillUnmount() {
    clearInterval(this.interval)
  }

  render() {
    return (
      <Ripple>
        <div className="bootstrap-wrapper">
          <div className="app-container container" ref={(el) => this.appContainer = el}>
            <div className="row">
              <div className="col-xs-6 col-sm-6 col-md-6 col-lg-6 col-xl-6">
                <h1>Heart Watch</h1>
              </div>
              <div className="col-xs-6 col-sm-6 col-md-6 col-lg-6 col-xl-6 buttons-right">
                <div className="row">
                  <div className="col-xs-9 col-sm-9 col-md-9 col-lg-9 col-xl-9">
{/*                    <form onSubmit={this.handleRun}>
                      <Input placeholder="Enter the secret to run" />
                      <input type="submit" className="k-button k-primary" value="Run" />
                    </form>
*/}                  </div>
                  <div className="col-xs-3 col-sm-3 col-md-3 col-lg-3 col-xl-3">
                    <Button onClick={this.handlePDFExport}>Export to PDF</Button>
                  </div>
                </div>
              </div>
            </div>
            <div className="row">
              <div className="col-xs-3 col-sm-3 col-md-3 col-lg-3 col-xl-3">
                <div className="percentage-container">
                  <span className="big-text">{this.state.brokers_info.amount}</span>
                  <span className="small-text">{this.state.brokers_info.instance_type}</span>
                  <p>Kafka cluster</p>
                </div>
              </div>
              <div className="col-xs-3 col-sm-3 col-md-3 col-lg-3 col-xl-3">
                <div className="percentage-container">
                  <span className="big-text">{this.state.ksqls_info.amount}</span>
                  <span className="small-text">{this.state.ksqls_info.instance_type}</span>
                  <p>KSQL cluster</p>
                </div>
              </div>
              <div className="col-xs-3 col-sm-3 col-md-3 col-lg-3 col-xl-3">
                <div className="percentage-container">
                  <span className="big-text">{this.state.num_parallel_producer_processes}</span>
                  <span className="small-text"></span>
                  <p>Num. parallel producer processes</p>
                </div>
              </div>
              <div className="col-xs-3 col-sm-3 col-md-3 col-lg-3 col-xl-3">
                <div className="percentage-container">
                  <span className="big-text">{this.state.num_partitions}</span>
                  <span className="small-text"></span>
                  <p>Num. partitions</p>
                </div>
              </div>
            </div>
            <div className="row">
              <div className="col-xs-6 col-sm-6 col-md-6 col-lg-6 col-xl-6">
                <KSQLMetricsPanel resource="interim_1"></KSQLMetricsPanel>
              </div>
              <div className="col-xs-6 col-sm-6 col-md-6 col-lg-6 col-xl-6">
                <KSQLMetricsPanel resource="interim_3"></KSQLMetricsPanel>
              </div>
            </div>
          </div>
        </div>
      </Ripple>
    )
  }
}

export default App
