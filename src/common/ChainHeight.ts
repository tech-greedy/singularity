const genesisTimestamp = 1598306400;

export function DateToHeight (date: Date) : number {
  const timestamp = date.getTime() / 1000;
  return TimestampToHeight(timestamp);
}

export function HeightToDate (height: number) : Date {
  const d = new Date();
  d.setTime(HeightToTimestamp(height) * 1000);
  return d;
}

export function HeightToTimestamp (height: number) : number {
  return (height * 30) + genesisTimestamp;
}

export function TimestampToHeight (timestamp: number) : number {
  return Math.floor((timestamp - genesisTimestamp) / 30);
}

export function HeightFromCurrentTime () : number {
  return TimestampToHeight(CurrentTimestamp());
}

export function CurrentTimestamp () : number {
  return Math.floor(Date.now() / 1000);
}
