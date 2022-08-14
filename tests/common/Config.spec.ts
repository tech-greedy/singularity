import config, { ConfigInitializer } from '../../src/common/Config';
import fs from 'fs-extra';
describe('Config', () => {
  describe('config', () => {
    describe('get', () => {
      it('should get a value', () => {
        config.test_key = {
          test_key_1: 'test_value_1',
        }
        expect(config.get('test_key.test_key_1')).toEqual('test_value_1');
      });
      it('should throw if the key does not exist', () => {
        expect(() => config.get('some_key.some_key_1')).toThrowError(/Config key some_key.some_key_1 not found/);
      });
    });
    describe('getOrDefault', () => {
      it('should get a value', () => {
        config.test_key = {
          test_key_1: 'test_value_1',
        }
        expect(config.getOrDefault('test_key.test_key_1', 'default_value')).toEqual('test_value_1');
      });
      it('should return the default value if the key does not exist', () => {
        expect(config.getOrDefault('some_key.some_key_1', 'default_value')).toEqual('default_value');
      });
    });
    describe('has', () => {
      it('should return true if the key exists', () => {
        config.test_key = {
          test_key_1: 'test_value_1',
        }
        expect(config.has('test_key.test_key_1')).toEqual(true);
      }),
      it('should return false if the key does not exist', () => {
        expect(config.has('some_key.some_key_1')).toEqual(false);
      });
    });
  })
  describe('ConfigInitializer', () => {
    describe('initialize', () => {
      beforeEach(() => {
        ConfigInitializer['initialized'] = false;
        ConfigInitializer.unwatchFile();
      })
      afterEach(() => {
        ConfigInitializer['initialized'] = false;
        ConfigInitializer.unwatchFile();
        delete process.env.SINGULARITY_PATH;
      })
      it ('should initialize the config with default values', () => {
        ConfigInitializer.initialize();
        expect(config.logging.console_level).toEqual('info');
      })
      it('should initialize using the config defined in environment variable', (done) => {
        process.env.SINGULARITY_PATH = '/tmp';
        fs.writeFileSync('/tmp/default.toml', "[logging]\n" +
          "console_level = 'error'");
        ConfigInitializer.initialize();
        ConfigInitializer.watchFile();
        expect(config.logging.console_level).toEqual('error');
        fs.writeFileSync('/tmp/default.toml', "[logging]\n" +
          "console_level = 'error2'");
        setTimeout(() => {
            expect(config.logging.console_level).toEqual('error2');
            done();
        }, 100);
      })
    });
  });
});

