import { readFile, writeFile } from 'node:fs/promises';

const packageJsonPath = new URL('../package.json', import.meta.url);
const packageJson = JSON.parse(await readFile(packageJsonPath, 'utf8'));
const runNumber = process.env.GITHUB_RUN_NUMBER;
const sha = process.env.GITHUB_SHA;

if (typeof runNumber !== 'string' || runNumber.length === 0) {
  throw new Error('GITHUB_RUN_NUMBER is required to prepare the canary npm version.');
}

if (typeof sha !== 'string' || sha.length < 7) {
  throw new Error('GITHUB_SHA is required to prepare the canary npm version.');
}

const baseVersion = String(packageJson.version).replace(/[-+].*$/u, '');
packageJson.version = `${baseVersion}-main.${runNumber}.${sha.slice(0, 7)}`;

await writeFile(packageJsonPath, `${JSON.stringify(packageJson, null, 2)}\n`, 'utf8');
process.stdout.write(`Prepared npm canary version ${packageJson.version}.\n`);
