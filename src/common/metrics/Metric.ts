export default interface Metric {
  type: string,
  values: {[key: string]: any }
}

export interface MetricEvent {
  timestamp: number,
  instance: string,
  type: string,
  values: {[key: string]: any },
}
