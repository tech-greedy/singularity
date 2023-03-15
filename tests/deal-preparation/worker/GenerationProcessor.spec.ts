import Datastore from '../../../src/common/Datastore';
import fs from 'fs-extra';
import { FileList } from '../../../src/common/model/InputFileList';
import path from 'path';
import DealPreparationWorker from '../../../src/deal-preparation/DealPreparationWorker';
import Utils from '../../Utils';
import GenerateCar from '../../../src/common/GenerateCar';
import { processGeneration } from '../../../src/deal-preparation/worker/GenerationProcessor';

fdescribe('GenerationProcessor', () => {
  let worker: DealPreparationWorker;
  let defaultTimeout: number;
  beforeAll(async () => {
    await Utils.initDatabase();
    worker = new DealPreparationWorker();
    GenerateCar.initialize();
    defaultTimeout = jasmine.DEFAULT_TIMEOUT_INTERVAL;
    jasmine.DEFAULT_TIMEOUT_INTERVAL = 15_000;
  });
  beforeEach(async () => {
    await Datastore.ScanningRequestModel.deleteMany();
    await Datastore.GenerationRequestModel.deleteMany();
    await Datastore.InputFileListModel.deleteMany();
    await Datastore.OutputFileListModel.deleteMany();
  });
  afterAll(async () => {
    for (const file of await fs.readdir('.')) {
      if (file.endsWith('.car')) {
        await fs.rm(file);
      }
    }
    await fs.rm('tests/subfolder1', { recursive: true, force: true });
    await fs.rm('unittest', { recursive: true, force: true });
    jasmine.DEFAULT_TIMEOUT_INTERVAL = defaultTimeout;
  })

  it('should generate commp, car files for dataset with > 10000 subfiles', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
    })
    await fs.mkdir('tests/subfolder1/subfolder2', { recursive: true });
    const fileList: FileList = []
    for (let i = 10000; i < 20000; ++i) {
      const p = path.join('tests/subfolder1/subfolder2', `${i}.txt`);
      await fs.writeFile(p, i.toString())
      fileList.push({
        path: p,
        size: (await fs.stat(p)).size
      })
    }
    const created = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      datasetName: 'name',
      path: 'tests/subfolder1',
      index: 0,
      status: 'active',
      outDir: '.',
    });
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 0,
      fileList
    })
    expect(await processGeneration(worker, created)).toEqual({
        finished: true
    })
    const generation = await Datastore.GenerationRequestModel.findById(created.id);
    expect(generation).toEqual(jasmine.objectContaining({
      pieceCid: 'baga6ea4seaqfeodfhw5t6qpmwaisq2bdgkzzslokpauxr3qtfaxlite6l3y5ueq',
      dataCid: 'bafybeiguxstjbsehe6v6xppz3siy32sijzvi6s53qgqlzvbtvpelrqmyau',
      carSize: 1088493
    }));
    const list = await Datastore.OutputFileListModel.find({
      generationId: generation!.id
    });
    expect(list.length).toEqual(11);
    expect(list[0].generatedFileList[2].path).toEqual('subfolder2/10000.txt');
    expect(list[10].generatedFileList[0].path).toEqual('subfolder2/19998.txt');
  })
  it('should generate commp, car files for S3 dataset', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
    })
    const created = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      datasetName: 'name',
      path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open',
      index: 0,
      status: 'active',
      outDir: '.',
      tmpDir: './tmpdir'
    });
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 0,
      fileList: [
        {
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
        }
      ]
    });
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 1,
      fileList: [
        {
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
          start: 100,
          end: 200
        }
      ]
    });

    expect(await processGeneration(worker, created)).toEqual(jasmine.objectContaining({
      finished: true
    }));
    const found = await Datastore.GenerationRequestModel.findById(created.id);
    expect(found).toEqual(jasmine.objectContaining({
      status: 'completed',
      dataCid: 'bafybeigr2gwdh6asl54yc4dslbvpud65rak7grpxw535k4cftojhlsz5we',
      pieceCid: 'baga6ea4seaqlf2c4sxxyybac2ufo4halfeoccpgr3gwjvcumdpgoq6b6gaxhymi',
      pieceSize: 65536,
    }));
    const outputFileList = await Datastore.OutputFileListModel.findOne({generationId: found!.id});
    expect(outputFileList).toEqual(jasmine.objectContaining({
      index: 0,
      generatedFileList: [
        jasmine.objectContaining({
          path: '',
          dir: true,
          cid: 'bafybeigr2gwdh6asl54yc4dslbvpud65rak7grpxw535k4cftojhlsz5we',
        }),
        jasmine.objectContaining({
          path: 'gdc-beataml1.0-crenolanib-phs001628-2-open',
          dir: true,
          cid: 'bafybeifrpl23gaugc62s4hdpdevwekyh2bnkxeadsud3buhxxrdg2wcpvq',
        }),
        jasmine.objectContaining({
          path: 'gdc-beataml1.0-crenolanib-phs001628-2-open/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
          dir: false,
          cid: 'bafkreidumeur3zaz6f2ozrjp4u33pa2iai6wyp3pvsvxvfphm775pipoaq',
        }),
        jasmine.objectContaining({
          path: 'gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e',
          dir: true,
          cid: 'bafybeidbuwnnjcjh65fnwibg4pzegbfwktpzvuiwlw46mfsws6lxeyjdcu',
        }),
        jasmine.objectContaining({
          path: 'gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
          start: 100,
          end: 200,
          dir: false,
          cid: 'bafkreignadrjfrmxbu6f4lyp7jixdzkvxrdl7rh23x5uuqmlnbalq3tzum',
        }),
      ]
    }))
  })
  it('should return early if the generation request is paused', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
    })
    const created = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      datasetName: 'name',
      path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open',
      index: 0,
      status: 'paused',
      outDir: '.',
      tmpDir: './tmpdir'
    });
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 0,
      fileList: [
        {
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
        },{
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
        }
      ]
    });
    const result = await processGeneration(worker, created);;
    expect(result).toEqual(jasmine.objectContaining({
      finished: false,
    }));
  });
  it('should return early if the generation request is removed', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
    })
    const created = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      datasetName: 'name',
      path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open',
      index: 0,
      status: 'active',
      outDir: '.',
      tmpDir: './tmpdir'
    });
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 0,
      fileList: [
        {
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
        },{
          path: 's3://gdc-beataml1.0-crenolanib-phs001628-2-open/d7410180-f387-46e6-b12f-de29d4fbae0e/Supplementary Data 3 final paper w map.xlsx',
          size: 41464,
        }
      ]
    });
    const promise = processGeneration(worker, created);
    await scanning.delete();
    const result = await promise;
    expect(result).toEqual(jasmine.objectContaining({
      finished: false,
    }));
  });
  it('should skip inaccessible files', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
    })
    const created = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      datasetName: 'name',
      path: 'unittest',
      index: 0,
      status: 'active',
      outDir: '.',
      tmpDir: './tmpdir',
      skipInaccessibleFiles: true
    });
    await fs.mkdirp('unittest');
    await fs.writeFile('unittest/test1.txt', 'test1');
    await fs.writeFile('unittest/test2.txt', 'test2');
    await fs.chmod('unittest/test1.txt', 0);
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 0,
      fileList: [
        {
          path: 'unittest/test1.txt',
          size: 5,
        },
        {
          path: 'unittest/test2.txt',
          size: 5,
        }
      ]
    })
    expect(await processGeneration(worker, created)).toEqual(jasmine.objectContaining({
      finished: true
    }));
    const generatedFileList = await Datastore.OutputFileListModel.findOne({
      generatedId: created.id
    });
    expect(generatedFileList!.generatedFileList.some(file => file.path.includes('test1.txt'))).toBeFalse();
  })
  it('should generate commp, car files', async () => {
    const scanning = await Datastore.ScanningRequestModel.create({
      name: 'name',
      status: 'completed'
    })
    const created = await Datastore.GenerationRequestModel.create({
      datasetId: scanning.id,
      datasetName: 'name',
      path: 'tests/test_folder',
      index: 0,
      status: 'active',
      outDir: '.',
      tmpDir: './tmpdir'
    });
    await Datastore.InputFileListModel.create({
      generationId: created.id,
      index: 0,
      fileList: [
        {
          path: 'tests/test_folder/a/1.txt',
          size: 3,
        },
        {
          path: 'tests/test_folder/b/2.txt',
          size: 27,
          start: 0,
          end: 9,
        }
      ]
    })
    expect(await processGeneration(worker, created)).toEqual(jasmine.objectContaining({
      finished: true
    }));
    const found = await Datastore.GenerationRequestModel.findById(created.id);
    expect(found).toEqual(jasmine.objectContaining({
      status: 'completed',
      dataCid: 'bafybeih2nwd66s7rstnbj4grzjw7re4lyhmx3auvphibbz7nalo4ygfypq',
      pieceCid: 'baga6ea4seaqoqixvkneyg6tzwfoqsmw33xdva3aywkawp6n5jd5tffjdmqrn6gy',
      pieceSize: 512,
    }));
    const generatedFileList = await Datastore.OutputFileListModel.findOne({
      generatedId: created.id
    });
    expect(generatedFileList).toEqual(jasmine.objectContaining({
      index: 0,
      generatedFileList: [
        jasmine.objectContaining({
          path: '',
          dir: true,
          cid: 'bafybeih2nwd66s7rstnbj4grzjw7re4lyhmx3auvphibbz7nalo4ygfypq',
        }),
        jasmine.objectContaining({
          path: 'a',
          dir: true,
          cid: 'bafybeifd34zco7545dzqflv7djpi3q2l2egi4l4coohgftgjssn4zoeu2y',
        }),
        jasmine.objectContaining({
          path: path.join('a', '1.txt'),
          size: 3,
          dir: false,
          cid: 'bafkreiey5jxe6ilpf62lnh77tm5ejbbmhbugzjuf6p2v3remlu73ced34q',
        }),
        jasmine.objectContaining({
          path: 'b',
          dir: true,
          cid: 'bafybeif7zaqg45xk5zvwybbfgeiotkzvjmd4bpjasb4aevne57dpt67com',
        }),
        jasmine.objectContaining({
          path: path.join('b', '2.txt'),
          size: 27,
          start: 0,
          end: 9,
          dir: false,
          cid: 'bafkreiblmv6wzk3grdk7u5a7u5zqh5vez3zatwuk3ptparw45unujqxysi',
        }),
      ]
    }))
  })
})
