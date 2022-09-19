import {
  DateToHeight,
  HeightFromCurrentTime,
  HeightToDate,
  HeightToTimestamp,
  TimestampToHeight
} from '../../src/common/ChainHeight';

describe('ChainHeight', () => {
  describe('DateToHeight', () => {
    it('should convert a date to a height', () => {
      const date = new Date(1660376130 * 1000);
      expect(DateToHeight(date)).toEqual(2068991);
    });
  });
  describe('HeightToDate', () => {
    it('should convert a height to a date', () => {
      const height = 2_068_991;
      expect(HeightToDate(height).toUTCString()).toEqual('Sat, 13 Aug 2022 07:35:30 GMT');
    });
  });
  describe('HeightToTimestamp', () => {
    it('should convert a height to a timestamp', () => {
      const height = 2_068_991;
      expect(HeightToTimestamp(height)).toEqual(1660376130);
    });
  });
  describe('TimestampToHeight', () => {
    it('should convert a timestamp to a height', () => {
      const timestamp = 1660376130;
      expect(TimestampToHeight(timestamp)).toEqual(2_068_991);
    });
  });
  describe('HeightFromCurrentTime', () => {
    it('should convert the current time to a height', () => {
      const height = TimestampToHeight(Date.now() / 1000);
      expect(HeightFromCurrentTime()).toEqual(height);
    });
  });
});
