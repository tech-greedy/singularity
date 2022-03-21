import glob from 'fast-glob';

const entries = glob.sync('**/*');
for (const entry of entries.sort()) {
  console.log(entry);
}
