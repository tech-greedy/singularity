import {DynamizeArray, DynamizeMap} from "../../src/index/FsDag";

function replacer(_key: any, value: any) {
  if(value instanceof Map) {
    return Object.fromEntries(value)
  } else {
    return value;
  }
}

describe('FsDag', () => {
  describe('DynamizeMap', () => {
    it('should return a map if the map is smaller than maxLink', () => {
      const map = new Map<string, string>();
      map.set('a', 'a');
      map.set('b', 'b');
      map.set('c', 'c');
      let result = DynamizeMap(map, 3)
      expect(result).toEqual(map);
      result = DynamizeMap(map, 4)
      expect(result).toEqual(map);
    });
    it('should return a dynamic map if the map is larger than maxLink', () => {
      const map = new Map<string, string>();
      map.set('k1', 'v1');
      map.set('k2', 'v2');
      map.set('k3', 'v3');
      map.set('k4', 'v4');
      map.set('k5', 'v5');
      map.set('k6', 'v6');
      map.set('k7', 'v7');
      map.set('k8', 'v8');
      map.set('k9', 'v9');
      map.set('k10', 'v10');
      let result = JSON.stringify(DynamizeMap(map, 3), replacer, 2)
      expect(result).toEqual(`[
  {
    "from": "k1",
    "to": "k8",
    "map": [
      {
        "from": "k1",
        "to": "k2",
        "map": {
          "k1": "v1",
          "k10": "v10",
          "k2": "v2"
        }
      },
      {
        "from": "k3",
        "to": "k5",
        "map": {
          "k3": "v3",
          "k4": "v4",
          "k5": "v5"
        }
      },
      {
        "from": "k6",
        "to": "k8",
        "map": {
          "k6": "v6",
          "k7": "v7",
          "k8": "v8"
        }
      }
    ]
  },
  {
    "from": "k9",
    "to": "k9",
    "map": {
      "k9": "v9"
    }
  }
]`);
    });
  })
  describe('DynamizeArray', () => {
    it ('should return the same array if it is smaller than maxLink', () => {
      const array = ['a', 'b', 'c'];
      let result = DynamizeArray(array, 3);
      expect(result).toEqual(array);
      result = DynamizeArray(array, 5);
      expect(result).toEqual(array);
    })

    it('should return a dynamic array if it is larger than maxLink', () => {
      const array = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'];
      const result = JSON.stringify(DynamizeArray(array, 3), null, 2);
      expect(result).toEqual(`[
  {
    "index": 0,
    "array": [
      {
        "index": 0,
        "array": [
          "1",
          "2",
          "3"
        ]
      },
      {
        "index": 3,
        "array": [
          "4",
          "5",
          "6"
        ]
      },
      {
        "index": 6,
        "array": [
          "7",
          "8",
          "9"
        ]
      }
    ]
  },
  {
    "index": 9,
    "array": [
      "10"
    ]
  }
]`)
    })
  })
})
