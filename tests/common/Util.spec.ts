import {getNextPowerOfTwo} from "../../src/common/Util";

describe('Util', () => {
  describe('getNextPowerOfTwo', () => {
    it('should return the next power of two', () => {
      expect(getNextPowerOfTwo(1)).toBe(1)
      expect(getNextPowerOfTwo(2)).toBe(2)
      expect(getNextPowerOfTwo(3)).toBe(4)
      expect(getNextPowerOfTwo(4)).toBe(4)
      expect(getNextPowerOfTwo(5)).toBe(8)
      expect(getNextPowerOfTwo(6)).toBe(8)
      expect(getNextPowerOfTwo(7)).toBe(8)
      expect(getNextPowerOfTwo(8)).toBe(8)
      expect(getNextPowerOfTwo(9)).toBe(16)
    })
  })
})
