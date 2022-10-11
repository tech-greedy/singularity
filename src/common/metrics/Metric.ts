export default interface Metric {
  type: string,
  values: {[key: string]: any }
}

export interface MetricEvent {
  timestamp: number,
  ip: string,
  instance: string,
  type: string,
  values: {[key: string]: any },
}
