export default interface Metric {
  name: string,
  timestamp?: Date
  values: {[key: string]: string | number | boolean }
}
