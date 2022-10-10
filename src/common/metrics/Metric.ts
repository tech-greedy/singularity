export default interface Metric {
  type: string,
  values: {[key: string]: string | number | boolean }
}

export interface MetricEvent {
  timestamp: number,
  ip: string,
  instance: string,
  type: string,
  key: string,
  value: string
}
