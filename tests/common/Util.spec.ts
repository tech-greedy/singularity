import { getNextPowerOfTwo, shuffle, sleep } from '../../src/common/Util';

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

  describe('sleep', () => {
    it('should sleep', async () => {
      const start = Date.now()
      await sleep(100)
      const end = Date.now()
      expect(end - start).toBeGreaterThanOrEqual(100)
    })
  })

  describe('shuffle', () => {
    it('should shuffle', () => {
      const array = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
      const arrayCopy = [...array]
      const shuffled = shuffle(array)
      expect(shuffled).not.toEqual(arrayCopy)
      expect(shuffled.sort()).toEqual(array.sort())
    })
  })
})
