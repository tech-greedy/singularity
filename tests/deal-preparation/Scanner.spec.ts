import Scanner from '../../src/deal-preparation/Scanner';
import path from 'path';

describe('Scanner', () => {
  describe('scan', () => {
    it('should work without startFile', async () => {
      const arr = [];
      for await (const list of await Scanner.scan(path.join('tests', 'test_folder'), 12, 16)){
        arr.push(list);
      }
      expect(arr).toEqual([
          [
            { size: 3, path: 'tests/test_folder/a/1.txt' },
            { size: 27, start: 0, end: 9, path: 'tests/test_folder/b/2.txt' }
          ],
          [
            { size: 27, start: 9, end: 21, path: 'tests/test_folder/b/2.txt' }
          ],
          [
            { size: 27, start: 21, end: 27, path: 'tests/test_folder/b/2.txt' },
            { size: 9, path: 'tests/test_folder/c/3.txt' }
          ],
          [ { size: 9, path: 'tests/test_folder/d.txt' } ]
        ]
      );
    })
    it('should work with startFile of full file', async () => {
      const arr = [];
      for await (const list of await Scanner.scan(path.join('tests', 'test_folder'), 12, 16, {
        size: 27, start: 0, end: 27, path: 'tests/test_folder/b/2.txt'
      })){
        arr.push(list);
      }
      expect(arr).toEqual([
          [
            { size: 9, path: 'tests/test_folder/c/3.txt' },
            { size: 9, start: 0, end: 3, path: 'tests/test_folder/d.txt' }
          ],
          [ { size: 9, start: 3, end: 9, path: 'tests/test_folder/d.txt' } ]
        ]
      );
    })
    it('should work with startFile of partial file', async () => {
      const arr = [];
      for await (const list of await Scanner.scan(path.join('tests', 'test_folder'), 12, 16, {
        size: 27, start: 0, end: 9, path: 'tests/test_folder/b/2.txt'
      })){
        arr.push(list);
      }
      expect(arr).toEqual([
          [
            { size: 27, start: 9, end: 21, path: 'tests/test_folder/b/2.txt' }
          ],
          [
            { size: 27, start: 21, end: 27, path: 'tests/test_folder/b/2.txt' },
            { size: 9, path: 'tests/test_folder/c/3.txt' }
          ],
          [ { size: 9, path: 'tests/test_folder/d.txt' } ]
        ]
      );
    })
  })
})
