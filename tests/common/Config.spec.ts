import config, { ConfigInitializer } from '../../src/common/Config';
import fs from 'fs-extra';
import { sleep } from '../../src/common/Util';
describe('Config', () => {
  afterAll(async () => {
    ConfigInitializer.unwatchFile();
    ConfigInitializer['initialized'] = false;
    await ConfigInitializer.initialize(true);
  })
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
      it ('should initialize the config with default values', async () => {
        await ConfigInitializer.initialize();
        expect(config.logging.console_level).toEqual('info');
        expect(ConfigInitializer.instanceId).toEqual('unknown');
        expect(ConfigInitializer.publicIp).toEqual('unknown');
      })
      it('should initialize using the config defined in environment variable and watch file change', async () => {
        process.env.SINGULARITY_PATH = '/tmp';
        fs.writeFileSync('/tmp/default.toml', "[logging]\n" +
          "console_level = 'error'");
        await ConfigInitializer.initialize();
        ConfigInitializer.watchFile();
        ConfigInitializer.watchFile();
        expect(config.logging.console_level).toEqual('error');
        fs.writeFileSync('/tmp/default.toml', "[logging]\n" +
          "console_level = 'error2'");
        await sleep(100);
        expect(config.logging.console_level).toEqual('error2');
      })
      it('should initialize using the config defined in environment variable and skip file rename', async () => {
        process.env.SINGULARITY_PATH = '/tmp';
        fs.writeFileSync('/tmp/default.toml', "[logging]\n" +
          "console_level = 'error'");
        await ConfigInitializer.initialize();
        ConfigInitializer.watchFile();
        expect(config.logging.console_level).toEqual('error');
        // Rename does nothing
        fs.moveSync('/tmp/default.toml', '/tmp/default2.toml', { overwrite: true});
        await sleep(100);
        expect(config.logging.console_level).toEqual('error');
      })
    });
  });
});

